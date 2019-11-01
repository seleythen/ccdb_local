package ch.alice.o2.ccdb.multicast;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.mail.BodyPart;
import javax.mail.internet.MimeMultipart;
import javax.mail.util.ByteArrayDataSource;
import javax.servlet.http.HttpServletResponse;

import alien.test.cassandra.tomcat.Options;
import ch.alice.o2.ccdb.multicast.Utils.Pair;

/**
 * @author ddosaru
 *
 */
public class UDPReceiver extends Thread {
	private static final Logger logger = SingletonLogger.getLogger();

	/**
	 * How soon to start the recovery of incomplete objects after the last received fragment
	 */
	private static final long DELTA_T = 5000;

	/**
	 * For how long superseded objects should be kept around, in case delayed processing needs them
	 */
	private static final long TTL_FOR_SUPERSEDED_OBJECTS = 1000 * 60 * 5;

	private String multicastIPaddress = null;
	private int multicastPortNumber = 0;

	private int unicastPortNumber = 0;

	private static String recoveryBaseURL = Options.getOption("udp_receiver.recovery_url", "http://alice-ccdb.cern.ch:8080/");

	static int nrPacketsReceived = 0;

	/**
	 * Blob-uri complete
	 */
	public static final Map<String, List<Blob>> currentCacheContent = new ConcurrentHashMap<>();

	/**
	 * Initialize the configuration
	 */
	public UDPReceiver() {
		multicastIPaddress = Options.getOption("udp_receiver.multicast_address", null);
		multicastPortNumber = Options.getIntOption("udp_receiver.multicast_port", 3342);

		unicastPortNumber = Options.getIntOption("udp_receiver.unicast_port", 0);
	}

	private final Thread counterThread = new Thread(new Runnable() {
		@Override
		public void run() {
			int oldNrPacketsReceived = 0;
			while (true) {
				try {
					Thread.sleep(1000);
				}
				catch (final InterruptedException e) {
					e.printStackTrace();
				}

				if (UDPReceiver.nrPacketsReceived - oldNrPacketsReceived > 0) {
					logger.log(Level.INFO, "Received " + (UDPReceiver.nrPacketsReceived - oldNrPacketsReceived) + " packets per second. \n" + "Total " + UDPReceiver.nrPacketsReceived);
					oldNrPacketsReceived = UDPReceiver.nrPacketsReceived;
				}

				// logger.log(Level.INFO, "Recovery queue length: " + recoveryQueue.size());
			}
		}
	});

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

	private static void recoverBlob(final Blob blob) {
		System.err.println("Started recovery of " + blob.getKey() + " / " + blob.getUuid());

		// ArrayList<Pair> metadataMissingBlocks = blob.getMissingMetadataBlocks();
		// TODO: Metadata recovery

		final ArrayList<Pair> payloadMissingBlocks = blob.getMissingPayloadBlocks();

		System.err.println("Have to ask for : " + payloadMissingBlocks);

		if (payloadMissingBlocks == null || payloadMissingBlocks.size() == 0) {
			// Recover the entire Blob
			try {
				final URL url = new URL(recoveryBaseURL + "download/" + blob.getUuid().toString());
				final HttpURLConnection con = (HttpURLConnection) url.openConnection();
				con.setRequestMethod("GET");

				con.setConnectTimeout(5000); // server should be fast (< 5 sec)
				con.setReadTimeout(5000);

				final int status = con.getResponseCode();
				if (status == HttpServletResponse.SC_OK) {
					try (BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()))) {
						String inputLine;
						final StringBuffer content = new StringBuffer();
						while ((inputLine = in.readLine()) != null) {
							content.append(inputLine);
						}

						blob.addByteRange(content.toString().getBytes(), new Pair(0, content.toString().getBytes().length));
					}
				}
				else {
					// TODO retry?
				}
			}
			catch (final Exception e) {
				e.printStackTrace();
			}

		}
		else {
			String ranges = "";
			int i = 0;
			for (i = 0; i < payloadMissingBlocks.size() - 1; i++) {
				ranges += payloadMissingBlocks.get(i).first;
				ranges += "-";
				ranges += payloadMissingBlocks.get(i).second;
				ranges += ",";
			}
			ranges += payloadMissingBlocks.get(i).first;
			ranges += "-";
			ranges += payloadMissingBlocks.get(i).second;

			System.err.println("Asking URL\n" + recoveryBaseURL + "download/" + blob.getUuid().toString() + "\nfor \nRange: bytes=" + ranges);

			try {
				final URL url = new URL(recoveryBaseURL + "download/" + blob.getUuid().toString());
				final HttpURLConnection con = (HttpURLConnection) url.openConnection();
				con.setRequestProperty("Range", "bytes=" + ranges);
				con.setRequestMethod("GET");

				con.setConnectTimeout(5000); // server should be fast (< 5 sec)
				con.setReadTimeout(5000);

				final int status = con.getResponseCode();

				System.err.println("Server response code: " + status);

				if (status == HttpServletResponse.SC_PARTIAL_CONTENT) {
					final ByteArrayDataSource dataSource = new ByteArrayDataSource(con.getInputStream(), con.getHeaderField("Content-Type"));
					final MimeMultipart mm = new MimeMultipart(dataSource);

					for (int part = 0; part < mm.getCount(); part++) {
						final BodyPart bp = mm.getBodyPart(part);

						final byte[] content = new byte[bp.getSize()];

						bp.getInputStream().read(content);

						final String contentRange = bp.getHeader("Content-Range")[0];

						final StringTokenizer st = new StringTokenizer(contentRange, " -/");

						st.nextToken();

						final int startOffset = Integer.parseInt(st.nextToken());

						System.err.println("Add range " + startOffset + " .. " + (startOffset + content.length));

						blob.addByteRange(content, new Pair(startOffset, startOffset + content.length));
					}
				}

				blob.recomputeIsComplete();
				
				System.err.println("After this blob is complete: " + blob.isComplete());
				System.err.println("Missing blocks: " + blob.getMissingPayloadBlocks());
			}
			catch (final Exception e) {
				logger.log(Level.WARNING, "Exception recovering " + blob.getUuid(), e);
			}
		}
	}

	private final Thread incompleteBlobRecovery = new Thread(new Runnable() {
		@Override
		public void run() {
			setName("IncompleteBlobRecovery");

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
				}
				catch (@SuppressWarnings("unused") NoSuchAlgorithmException | IOException e1) {
					// nothing
				}

				recoverBlob(blob);
			}

		}
	});

	private static Comparator<Blob> startTimeComparator = new Comparator<>() {
		@Override
		public int compare(final Blob o1, final Blob o2) {
			final long diff = o1.startTime - o2.startTime;

			if (diff > 0)
				return 1;

			if (diff < 0)
				return -1;

			return 0;
		}
	};

	/**
	 * Add the complete Blob to the cache
	 *
	 * @param blob
	 */
	public static void addToCacheContent(final Blob blob) {
		final List<Blob> currentBlobsForKey = currentCacheContent.computeIfAbsent(blob.getKey(), k -> new Vector<>());

		synchronized (currentBlobsForKey) {
			if (!currentBlobsForKey.contains(blob))
				currentBlobsForKey.add(blob);

			currentBlobsForKey.sort(startTimeComparator);
		}
	}

	private static void processPacket(final FragmentedBlob fragmentedBlob) throws NoSuchAlgorithmException, IOException {
		// System.out.println("Fragment payload offset " + fragmentedBlob.getFragmentOffset() + " size " + fragmentedBlob.getblobDataLength());

		final List<Blob> candidates = currentCacheContent.get(fragmentedBlob.getKey());

		Blob blob = null;

		if (candidates != null)
			for (final Blob b : candidates)
				if (b.getUuid().equals(fragmentedBlob.getUuid())) {
					blob = b;
					break;
				}

		if (blob != null && blob.isComplete()) {
			// the complete object was already in memory, keep it and ignore retransmissions of other fragments of the same
			return;
		}

		if (blob == null) {
			// no in flight object yet
			blob = new Blob(fragmentedBlob.getKey(), fragmentedBlob.getUuid());

			addToCacheContent(blob);
		}

		blob.addFragmentedBlob(fragmentedBlob);
		blob.recomputeIsComplete();

		final DelayedBlob delayedBlob = new DelayedBlob(blob);

		if (blob.isComplete()) {
			System.err.println("Object is now complete, removing from notification queue");

			recoveryQueue.remove(delayedBlob);

			// just to correctly sort by start time once it is computed by Blob.isComplete()
			addToCacheContent(blob);

			nrPacketsReceived++;
		}
		else {
			synchronized (recoveryQueue) {
				if (!recoveryQueue.contains(delayedBlob))
					recoveryQueue.offer(delayedBlob);
			}
		}
	}

	private static ExecutorService executorService = null;

	private static void queueProcessing(final FragmentedBlob fragmentedBlob) {
		executorService.submit(() -> {
			try {
				processPacket(fragmentedBlob);
			}
			catch (NoSuchAlgorithmException | IOException e) {
				e.printStackTrace();
			}
		});
	}

	private void runMulticastReceiver() {
		try (MulticastSocket socket = new MulticastSocket(this.multicastPortNumber)) {
			final InetAddress group = InetAddress.getByName(this.multicastIPaddress);
			socket.joinGroup(group);
			this.counterThread.start();

			while (true) {
				try {
					final byte[] buf = new byte[Utils.PACKET_MAX_SIZE];
					// Receive object
					final DatagramPacket packet = new DatagramPacket(buf, buf.length);
					socket.receive(packet);

					// System.out.println("Received one fragment on multicast");

					queueProcessing(new FragmentedBlob(buf, packet.getLength()));

					// System.out.println("cacheContent: " + currentCacheContent.size());
				}
				catch (final Exception e) {
					// logger.log(Level.WARNING, "Exception thrown");
					e.printStackTrace();
				}
			}
			// socket.leaveGroup(group);
		}
		catch (final IOException e) {
			logger.log(Level.SEVERE, "Exception running the multicast receiver", e);
		}
	}

	private void runUnicastReceiver() {
		setName("UnicastReceiver");

		try (DatagramSocket serverSocket = new DatagramSocket(this.unicastPortNumber)) {
			while (true) {
				try {
					final byte[] buf = new byte[Utils.PACKET_MAX_SIZE];
					// Receive object
					final DatagramPacket packet = new DatagramPacket(buf, buf.length);
					serverSocket.receive(packet);

					// System.out.println("Received one fragment on unicast");

					queueProcessing(new FragmentedBlob(buf, packet.getLength()));

					// System.out.println("cacheContent: " + currentCacheContent.size());
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
				final Iterator<Map.Entry<String, List<Blob>>> cacheContentIterator = currentCacheContent.entrySet().iterator();

				while (cacheContentIterator.hasNext()) {
					final Map.Entry<String, List<Blob>> currentEntry = cacheContentIterator.next();

					final List<Blob> objects = currentEntry.getValue();

					if (objects.size() == 0) {
						cacheContentIterator.remove();
						continue;
					}

					final long currentTime = System.currentTimeMillis();

					synchronized (objects) {
						final Iterator<Blob> objectIterator = objects.iterator();

						while (objectIterator.hasNext()) {
							final Blob b = objectIterator.next();

							try {
								if (b.isComplete()) {
									if (b.getEndTime() < currentTime) {
										if (logger.isLoggable(Level.INFO))
											logger.log(Level.INFO, "Removing expired object for " + b.getKey() + ": " + b.getUuid() + " (expired " + b.getEndTime() + ")");

										objectIterator.remove();
									}
								}
								else
									if (System.currentTimeMillis() - b.getLastTouched() > 1000 * 60)
										objectIterator.remove();
							}
							catch (@SuppressWarnings("unused") final NoSuchAlgorithmException e) {
								// ignore
							}
							catch (@SuppressWarnings("unused") final IOException e) {
								// checksum is wrong, drop the hot potato
								objectIterator.remove();
							}
						}

						if (objects.size() <= 1) {
							// if empty the list will stay around one more iteration, will be collected by the above conditions then
							continue;
						}

						for (int i = 0; i < objects.size() - 1; i++) {
							final Blob b = objects.get(i);

							try {
								if (!b.isComplete())
									continue;
							}
							catch (@SuppressWarnings("unused") NoSuchAlgorithmException | IOException e) {
								// ignore
							}

							if (currentTime - b.getStartTime() > TTL_FOR_SUPERSEDED_OBJECTS) {
								// more than 5 minutes old and superseded by a newer one, can be removed
								if (logger.isLoggable(Level.INFO))
									logger.log(Level.INFO, "Removing superseded object for " + b.getKey() + ": " + b.getUuid() + " (valid since " + b.getStartTime() + "):\n" + b);

								objects.remove(i);
							}
							else
								break;
						}
					}
				}

				try {
					sleep(1000);
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
		boolean anyListenerStarted = false;

		if (multicastIPaddress != null && multicastIPaddress.length() > 0 && multicastPortNumber > 0) {
			System.err.println("Starting multicast receiver on " + multicastIPaddress + ":" + multicastPortNumber);

			new Thread(() -> runMulticastReceiver()).start();

			anyListenerStarted = true;
		}
		else
			System.err.println("Not starting the multicast receiver");

		if (unicastPortNumber > 0) {
			System.err.println("Starting unicast receiver on " + unicastPortNumber);

			new Thread(() -> runUnicastReceiver()).start();

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

		executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	}

}
