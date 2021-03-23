package ch.alice.o2.ccdb.multicast;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.mail.BodyPart;
import javax.mail.internet.MimeMultipart;
import javax.mail.util.ByteArrayDataSource;
import javax.servlet.http.HttpServletResponse;

import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.test.cassandra.tomcat.Options;
import ch.alice.o2.ccdb.multicast.Utils.Pair;
import utils.CachedThreadPool;

/**
 * @author ddosaru
 *
 */
public class UDPReceiver extends Thread {
	private static final Logger logger = SingletonLogger.getLogger();

	private static final Monitor monitor = MonitorFactory.getMonitor(UDPReceiver.class.getCanonicalName());

	/**
	 * How soon to start the recovery of incomplete objects after the last received fragment
	 */
	private static final long DELTA_T = Options.getIntOption("udp_receiver.recovery_after_ms", 2000);

	/**
	 * For how long superseded objects should be kept around, in case delayed processing needs them
	 */
	private static final long TTL_FOR_SUPERSEDED_OBJECTS = 1000 * 60 * 2;

	private String multicastIPaddress = null;

	private String multicastInterface = null;

	private int multicastPortNumber = 0;

	private int unicastPortNumber = 0;

	private static String recoveryBaseURL = Options.getOption("udp_receiver.recovery_url", "http://alice-ccdb.cern.ch:8080/");

	/**
	 * Blob-uri complete
	 */
	public static final Map<String, List<Reference<Blob>>> currentCacheContent = new ConcurrentHashMap<>();

	private static final ReentrantReadWriteLock contentStructureLock = new ReentrantReadWriteLock();

	private static final ReadLock contentStructureReadLock = contentStructureLock.readLock();
	private static final WriteLock contentStructureWriteLock = contentStructureLock.writeLock();

	private static final int SOFT_REFERENCE_THRESHOLD = Options.getIntOption("udpreceiver.soft_references_threshold", 10);

	/**
	 * Initialize the configuration
	 */
	public UDPReceiver() {
		// listen by default on multicast 224.0.204.219:3342, but not on UDP unicast

		multicastIPaddress = Options.getOption("udp_receiver.multicast_address", "224.0.204.219"); // 224.0.0xCC.0xDB
		multicastPortNumber = Options.getIntOption("udp_receiver.multicast_port", 3342);
		multicastInterface = Options.getOption("udp_receiver.multicast_interface", "");

		unicastPortNumber = Options.getIntOption("udp_receiver.unicast_port", 0);
	}

	/**
	 * @author costing
	 * @since 2019-11-01
	 */
	private static class DelayedBlob implements Delayed {
		public Blob blob;

		public DelayedBlob(final Blob blob) {
			this.blob = blob;
		}

		@Override
		public boolean equals(final Object obj) {
			if (!(obj instanceof DelayedBlob))
				return false;

			final DelayedBlob oth = (DelayedBlob) obj;

			return this.blob.getUuid().equals(oth.blob.getUuid());
		}

		@Override
		public int compareTo(final Delayed o) {
			final long diff = this.blob.getLastTouched() - ((DelayedBlob) o).blob.getLastTouched();

			if (diff < 0)
				return -1;

			if (diff > 0)
				return 1;

			return 0;
		}

		@Override
		public long getDelay(final TimeUnit unit) {
			final long msToRecovery = blob.getLastTouched() + DELTA_T - System.currentTimeMillis();

			return unit.convert(msToRecovery, TimeUnit.MILLISECONDS);
		}

		@Override
		public int hashCode() {
			return blob.hashCode();
		}
	}

	private static DelayQueue<DelayedBlob> recoveryQueue = new DelayQueue<>();

	private static boolean recoverBlob(final Blob blob) {
		// ArrayList<Pair> metadataMissingBlocks = blob.getMissingMetadataBlocks();
		// TODO: Metadata recovery

		final ArrayList<Pair> payloadMissingBlocks = blob.getMissingPayloadBlocks();

		if (payloadMissingBlocks == null) {
			// Recover the entire Blob
			try (Timing t = new Timing(monitor, "fullRecovery_ms")) {
				final URL url = new URL(recoveryBaseURL + "download/" + blob.getUuid().toString());
				final HttpURLConnection con = (HttpURLConnection) url.openConnection();
				con.setRequestMethod("GET");

				con.setConnectTimeout(5000); // server should be fast (< 5 sec)
				con.setReadTimeout(5000);

				final int status = con.getResponseCode();
				if (status == HttpServletResponse.SC_OK) {
					copyHeaders(con, blob);

					final byte[] payload = new byte[con.getContentLength()];
					int offset = 0;

					monitor.addMeasurement("missingBytes", con.getContentLength());

					try (InputStream input = con.getInputStream(); ByteArrayOutputStream baos = new ByteArrayOutputStream(con.getContentLength())) {
						int leftToRead;

						while ((leftToRead = payload.length - offset) > 0) {
							final int read = input.read(payload, offset, leftToRead);

							if (read <= 0) {
								break;
							}

							offset += read;
						}

						blob.setPayload(payload);
					}
				}

				blob.recomputeIsComplete();

				return blob.isComplete();
			}
			catch (final Exception e) {
				logger.log(Level.WARNING, "Exception recovering full content of " + blob.getKey() + " / " + blob.getUuid(), e);
				return false;
			}
		}

		if (payloadMissingBlocks.size() == 0) {
			// Just the metadata is incomplete, ask for the header
			try (Timing t = new Timing(monitor, "headersRecovery_ms")) {
				final URL url = new URL(recoveryBaseURL + "download/" + blob.getUuid().toString());
				final HttpURLConnection con = (HttpURLConnection) url.openConnection();
				con.setRequestMethod("HEAD");

				con.setConnectTimeout(5000); // server should be fast (< 5 sec)
				con.setReadTimeout(5000);

				final int status = con.getResponseCode();

				if (status == HttpServletResponse.SC_OK)
					copyHeaders(con, blob);

				blob.recomputeIsComplete();

				return blob.isComplete();
			}
			catch (final Exception e) {
				logger.log(Level.WARNING, "Exception recovering headers of " + blob.getKey() + " / " + blob.getUuid(), e);
				return false;
			}
		}

		final StringBuilder ranges = new StringBuilder();
		int i = 0;

		int missingBytes = 0;

		for (i = 0; i < payloadMissingBlocks.size(); i++) {
			if (ranges.length() > 0)
				ranges.append(',');

			final Pair range = payloadMissingBlocks.get(i);

			ranges.append(range.first).append('-').append(range.second);

			missingBytes += range.second - range.first + 1;
		}

		System.err.println("Asking URL " + recoveryBaseURL + "download/" + blob.getUuid().toString() + " for \n  Range: bytes=" + ranges);

		monitor.addMeasurement("missingBytes", missingBytes);

		try (Timing t = new Timing(monitor, "rangeRecovery_ms")) {
			final URL url = new URL(recoveryBaseURL + "download/" + blob.getUuid().toString());
			final HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestProperty("Range", "bytes=" + ranges);
			con.setRequestMethod("GET");

			con.setConnectTimeout(5000); // server should be fast (< 5 sec)
			con.setReadTimeout(5000);

			final int status = con.getResponseCode();

			if (status == HttpServletResponse.SC_PARTIAL_CONTENT) {
				copyHeaders(con, blob);

				if (payloadMissingBlocks.size() > 1) {
					// more than one Range will come as multipart responses

					try (InputStream conIS = con.getInputStream()) {
						final ByteArrayDataSource dataSource = new ByteArrayDataSource(conIS, con.getHeaderField("Content-Type"));

						final MimeMultipart mm = new MimeMultipart(dataSource);

						for (int part = 0; part < mm.getCount(); part++) {
							final BodyPart bp = mm.getBodyPart(part);

							final String contentRange = bp.getHeader("Content-Range")[0];

							final byte[] content = new byte[bp.getSize()];

							try (InputStream is = bp.getInputStream()) {
								is.read(content);
							}

							final StringTokenizer st = new StringTokenizer(contentRange, " -/");

							st.nextToken();

							final int startOffset = Integer.parseInt(st.nextToken());

							blob.addByteRange(content, new Pair(startOffset, startOffset + content.length));
						}
					}
				}
				else {
					// a single Range comes inline as the body of the response

					final byte[] payloadFragment = new byte[con.getContentLength()];
					int offset = 0;

					try (InputStream input = con.getInputStream(); ByteArrayOutputStream baos = new ByteArrayOutputStream(con.getContentLength())) {
						int leftToRead;

						while ((leftToRead = payloadFragment.length - offset) > 0) {
							final int read = input.read(payloadFragment, offset, leftToRead);

							if (read <= 0) {
								break;
							}

							offset += read;
						}

						blob.addByteRange(payloadFragment, new Pair(payloadMissingBlocks.get(0).first, payloadMissingBlocks.get(0).first + payloadFragment.length));
					}
				}
			}

			blob.recomputeIsComplete();

			final boolean isNowComplete = blob.isComplete();

			System.err.println("  Blob " + blob.getUuid() + " after recovery is complete: " + isNowComplete);

			return isNowComplete;
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Exception recovering ranges from " + blob.getKey() + " / " + blob.getUuid(), e);
			return false;
		}
	}

	private static final Set<String> IGNORED_HEADERS = Set.of("Accept-Ranges", "Date", "ETag", "Content-Length", "Content-Type", "Content-Range");

	private static void copyHeaders(final HttpURLConnection con, final Blob blob) {
		final Map<String, List<String>> headers = con.getHeaderFields();

		if (headers == null || headers.size() == 0)
			return;

		for (final Map.Entry<String, List<String>> entry : headers.entrySet()) {
			String key = entry.getKey();

			if (key == null || IGNORED_HEADERS.contains(key) || key.equals("Content-Type") || entry.getValue() == null || entry.getValue().size() == 0)
				continue;

			String value = entry.getValue().get(entry.getValue().size() - 1); // last value overrides any previous ones

			if (key.equals("Content-Disposition")) {
				final int idx = value.indexOf("filename=\"");

				if (idx >= 0) {
					value = value.substring(idx + 10, value.indexOf('"', idx + 10));
					key = "OriginalFileName";
				}
				else
					break;
			}

			final String oldValue = blob.getProperty(key);

			if (oldValue == null || !oldValue.equals(value))
				blob.setProperty(key, value);
		}
	}

	private final Thread incompleteBlobRecovery = new Thread(new Runnable() {
		@Override
		public void run() {
			while (true) {
				DelayedBlob toRecover;
				try {
					toRecover = recoveryQueue.take();
				}
				catch (@SuppressWarnings("unused") final InterruptedException e1) {
					return;
				}

				if (toRecover == null)
					continue;

				final Blob blob = toRecover.blob;

				try {
					if (blob.isComplete()) {
						// nothing to do anymore
						continue;
					}

					if (recoverBlob(blob)) {
						monitor.incrementCounter("recovered_blobs");
						sort(blob.getKey());
					}
					else
						monitor.incrementCounter("failed_to_recover_blobs");
				}
				catch (final Exception e) {
					logger.log(Level.WARNING, "Exception running the recovery for " + blob.getKey() + " / " + blob.getUuid(), e);
				}
			}
		}
	}, "IncompleteBlobRecovery");

	private static final Comparator<Reference<Blob>> startTimeComparator = new Comparator<>() {
		@Override
		public int compare(final Reference<Blob> sb1, final Reference<Blob> sb2) {
			final Blob o1 = sb1.get();
			final Blob o2 = sb2.get();

			if (o1 == null && o2 == null)
				return 0;

			if (o1 == null)
				return -1;

			if (o2 == null)
				return 1;

			final long diff = o1.startTime - o2.startTime;

			if (diff > Integer.MAX_VALUE)
				return Integer.MAX_VALUE;

			if (diff > 0)
				return (int) diff;

			if (diff < Integer.MIN_VALUE)
				return Integer.MIN_VALUE;

			if (diff < 0)
				return (int) diff;

			return 0;
		}
	};

	/**
	 * Add the complete Blob to the cache
	 *
	 * @param blob
	 * @return <code>null</code> if it was added, otherwise a pointer to the existing object
	 */
	public static Blob addToCacheContent(final Blob blob) {
		contentStructureWriteLock.lock();

		try {
			final List<Reference<Blob>> currentBlobsForKey = currentCacheContent.computeIfAbsent(blob.getKey(), k -> new Vector<>(Arrays.asList(new SoftReference<>(blob))));

			for (final Reference<Blob> sb : currentBlobsForKey) {
				final Blob b = sb.get();

				if (b != null && b.getUuid().equals(blob.getUuid()))
					return b;
			}

			currentBlobsForKey.add(new SoftReference<>(blob));
			currentBlobsForKey.sort(startTimeComparator);

			// first entries are the oldest
			for (int i = 0; i < currentBlobsForKey.size() - SOFT_REFERENCE_THRESHOLD; i++) {
				final Reference<Blob> existing = currentBlobsForKey.get(i);

				if (existing instanceof SoftReference) {
					final Blob b = existing.get();

					if (b != null)
						currentBlobsForKey.set(i, new WeakReference<>(b));
					else {
						currentBlobsForKey.remove(i);
						i--;
					}
				}
			}

			return blob;
		}
		finally {
			contentStructureWriteLock.unlock();
		}
	}

	private static void sort(final String key) {
		contentStructureReadLock.lock();

		try {
			final List<Reference<Blob>> currentBlobsForKey = currentCacheContent.get(key);

			if (currentBlobsForKey != null)
				currentBlobsForKey.sort(startTimeComparator);
		}
		finally {
			contentStructureReadLock.unlock();
		}
	}

	private static void processPacket(final byte[] packet) throws NoSuchAlgorithmException, IOException {
		// System.out.println("Fragment payload offset " + fragmentedBlob.getFragmentOffset() + " size " + fragmentedBlob.getblobDataLength());
		final FragmentedBlob fragmentedBlob = new FragmentedBlob(packet, packet.length);

		contentStructureReadLock.lock();

		Blob blob = null;

		try {
			final List<Reference<Blob>> candidates = currentCacheContent.get(fragmentedBlob.getKey());

			if (candidates != null)
				for (final Reference<Blob> sb : candidates) {
					final Blob b = sb.get();

					if (b != null && b.getUuid().equals(fragmentedBlob.getUuid())) {
						blob = b;
						break;
					}
				}
		}
		finally {
			contentStructureReadLock.unlock();
		}

		if (blob != null && blob.isComplete()) {
			// the complete object was already in memory, keep it and ignore retransmissions of other fragments of the same
			return;
		}

		if (blob == null) {
			// if the synchronized method finds it when actually adding, this is the old blob
			blob = addToCacheContent(new Blob(fragmentedBlob.getKey(), fragmentedBlob.getUuid()));
		}

		blob.addFragmentedBlob(fragmentedBlob);
		blob.recomputeIsComplete();

		final DelayedBlob delayedBlob = new DelayedBlob(blob);

		if (blob.isComplete()) {
			recoveryQueue.remove(delayedBlob);

			System.err.println("Object " + blob.getUuid() + " was fully received");

			// just to correctly sort by start time once it is computed by Blob.isComplete()
			sort(blob.getKey());

			monitor.incrementCounter("fullyReceivedObjects");
		}
		else {
			// pick any static field object to synchronize on, just not the queue itself
			synchronized (recoveryBaseURL) {
				// remove the notification
				recoveryQueue.remove(delayedBlob);

				// add it back, with the new time to start recovery
				recoveryQueue.offer(delayedBlob);
			}
		}
	}

	private static ExecutorService executorService = null;

	private static void queueProcessing(final byte[] packet) {
		executorService.submit(() -> {
			try {
				processPacket(packet);
			}
			catch (NoSuchAlgorithmException | IOException e) {
				e.printStackTrace();
			}
		});
	}

	private void runMulticastReceiver() {
		try (MulticastSocket socket = new MulticastSocket(this.multicastPortNumber)) {

			final NetworkInterface netInterface = getInterface();
			final InetAddress group = InetAddress.getByName(this.multicastIPaddress);
			if(netInterface == null) {
				System.err.println("Join to default interface");
				socket.joinGroup(group);
			} else {
				System.err.println("Join to custom interface: " + netInterface.getName());
				final InetSocketAddress addr = new InetSocketAddress(group, 0);
				socket.joinGroup(addr, netInterface);
			}
			

			final byte[] buf = new byte[Utils.PACKET_MAX_SIZE];
			final DatagramPacket packet = new DatagramPacket(buf, buf.length);

			while (true) {
				try {
					// Receive object

					socket.receive(packet);
					final int len = packet.getLength();

					final byte[] activePart = new byte[len];
					System.arraycopy(buf, 0, activePart, 0, len);

					queueProcessing(activePart);

					monitor.addMeasurement("multicast_packets", len);
				}
				catch (final Exception e) {
					// logger.log(Level.WARNING, "Exception thrown");
					e.printStackTrace();
				}
			}
		}
		catch (final IOException e) {
			logger.log(Level.SEVERE, "Exception running the multicast receiver", e);
		}
	}

	private NetworkInterface getInterface() {
		try {
			Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
			for(NetworkInterface net: Collections.list(nets)) {
				if(net.getName().equals(multicastInterface) && !multicastInterface.equals("")) {
					return net;
				}
			}
			for(NetworkInterface net: Collections.list(nets)) {
				if(net.isUp() && net.supportsMulticast() && !net.isLoopback()) {
					return net;
				}
			}
			for(NetworkInterface net: Collections.list(nets)) {
				if(net.isUp() && net.supportsMulticast()) {
					return net;
				}
			}
			return null;
		} catch(SocketException se) {
			return null;
		}
	}

	private void runUnicastReceiver() {
		try (DatagramSocket serverSocket = new DatagramSocket(this.unicastPortNumber)) {
			final byte[] buf = new byte[Utils.PACKET_MAX_SIZE];
			final DatagramPacket packet = new DatagramPacket(buf, buf.length);

			while (true) {
				try {
					serverSocket.receive(packet);

					final int len = packet.getLength();

					final byte[] activePart = new byte[len];
					System.arraycopy(buf, 0, activePart, 0, len);

					queueProcessing(activePart);

					monitor.addMeasurement("unicast_packets", len);
				}
				catch (final Exception e) {
					// logger.log(Level.WARNING, "Exception thrown");
					e.printStackTrace();
				}
			}
		}
		catch (final IOException e) {
			logger.log(Level.SEVERE, "Exception running the unicast receiver", e);
		}
	}

	private static class ExpirationChecker extends Thread {
		public ExpirationChecker() {
			setName("InMemExpirationChecker");
		}

		@Override
		public void run() {
			while (true) {
				contentStructureWriteLock.lock();

				long seriesInMemory = 0;
				long objectsInMemory = 0;
				long sizeOfObjectsInMemory = 0;

				try {
					final Iterator<Map.Entry<String, List<Reference<Blob>>>> cacheContentIterator = currentCacheContent.entrySet().iterator();

					while (cacheContentIterator.hasNext()) {
						final Map.Entry<String, List<Reference<Blob>>> currentEntry = cacheContentIterator.next();

						final List<Reference<Blob>> objects = currentEntry.getValue();

						if (objects.size() == 0) {
							monitor.incrementCounter("cleaned_empty_lists");

							cacheContentIterator.remove();
							continue;
						}

						seriesInMemory++;

						final long currentTime = System.currentTimeMillis();

						synchronized (objects) {
							final Iterator<Reference<Blob>> objectIterator = objects.iterator();

							while (objectIterator.hasNext()) {
								final Reference<Blob> sb = objectIterator.next();

								final Blob b = sb.get();

								if (b == null) {
									monitor.incrementCounter("evicted_gc_objects");

									objectIterator.remove();
									continue;
								}

								try {
									if (b.isComplete()) {
										if (b.getEndTime() < currentTime) {
											if (logger.isLoggable(Level.INFO))
												logger.log(Level.INFO, "Removing expired object for " + b.getKey() + ": " + b.getUuid() + " (expired " + b.getEndTime() + ")");

											monitor.incrementCounter("evicted_expired_objects");

											objectIterator.remove();
										}
									}
									else
										if (System.currentTimeMillis() - b.getLastTouched() > 1000 * 10) {
											if (logger.isLoggable(Level.INFO))
												logger.log(Level.INFO, "Removing incomplete and not yet recovered object " + b.getKey() + ": " + b.getUuid());

											monitor.incrementCounter("evicted_incomplete_objects");

											objectIterator.remove();
										}
								}
								catch (@SuppressWarnings("unused") final NoSuchAlgorithmException e) {
									// ignore
								}
								catch (@SuppressWarnings("unused") final IOException e) {
									// checksum is wrong, drop the hot potato
									objectIterator.remove();
								}
							}

							// the most recent object should stay in any case
							for (int i = 0; i < objects.size() - 1; i++) {
								final Reference<Blob> sb = objects.get(i);

								final Blob b = sb.get();

								try {
									if (b == null || !b.isComplete())
										continue;
								}
								catch (@SuppressWarnings("unused") NoSuchAlgorithmException | IOException e) {
									// ignore
								}

								if (currentTime - b.getStartTime() > TTL_FOR_SUPERSEDED_OBJECTS) {
									// more than 5 minutes old and superseded by a newer one, can be removed
									if (logger.isLoggable(Level.INFO))
										logger.log(Level.INFO, "Removing superseded object for " + b.getKey() + ": " + b.getUuid() + " (valid since " + b.getStartTime() + "):\n" + b);

									monitor.incrementCounter("evicted_superseded_objects");

									objects.remove(i);
								}
							}

							for (final Reference<Blob> activeObject : objects) {
								final Blob b = activeObject.get();

								if (b != null) {
									objectsInMemory++;
									sizeOfObjectsInMemory += b.getSize();
								}
							}
						}
					}
				}
				finally {
					contentStructureWriteLock.unlock();
				}

				monitor.sendParameter("active_series_cnt", Double.valueOf(seriesInMemory));
				monitor.sendParameter("objects_in_memory_cnt", Double.valueOf(objectsInMemory));
				monitor.sendParameter("objects_in_memory_size", Double.valueOf(sizeOfObjectsInMemory));

				try {
					sleep(15000);
				}
				catch (@SuppressWarnings("unused") final InterruptedException e) {
					return;
				}
			}
		}
	}

	private static ExpirationChecker expirationChecker = null;

	@Override
	public void run() {
		executorService = new CachedThreadPool(Options.getIntOption("udp_receiver.threads", 4), 1, TimeUnit.MINUTES, (r) -> new Thread(r, "UDPPacketProcessor"));

		boolean anyListenerStarted = false;

		if (multicastIPaddress != null && multicastIPaddress.length() > 0 && multicastPortNumber > 0) {
			System.err.println("Starting multicast receiver on " + multicastIPaddress + ":" + multicastPortNumber);

			new Thread(() -> runMulticastReceiver(), "MulticastReceiver").start();

			anyListenerStarted = true;
		}
		else
			System.err.println("Not starting the multicast receiver");

		if (unicastPortNumber > 0) {
			System.err.println("Starting unicast receiver on " + unicastPortNumber);

			new Thread(() -> runUnicastReceiver(), "UnicastReceiver").start();

			anyListenerStarted = true;
		}
		else
			System.err.println("Not starting unicast receiver");

		if (anyListenerStarted)
			if (recoveryBaseURL != null && recoveryBaseURL.length() > 0) {
				System.err.println("Starting recovery of lost packets from " + recoveryBaseURL);

				incompleteBlobRecovery.start();
			}
			else
				System.err.println("No recovery URL defined");

		expirationChecker = new ExpirationChecker();
		expirationChecker.start();
	}
}
