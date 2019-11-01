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
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

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
	private static final long DELTA_T = 500;

	/**
	 * For how long superseded objects should be kept around, in case delayed processing needs them
	 */
	private static final long TTL_FOR_SUPERSEDED_OBJECTS = 1000 * 60 * 5;

	private String multicastIPaddress = null;
	private int multicastPortNumber = 0;

	private int unicastPortNumber = 0;

	private String recoveryBaseURL = null;

	static int nrPacketsReceived = 0;

	/**
	 * uuid <-> Blob fragmentat, zona de tranzitie pana la primirea completa a tuturor fragmentelor
	 */
	private final Map<UUID, Blob> inFlight = new ConcurrentHashMap<>();

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

		recoveryBaseURL = Options.getOption("udp_receiver.recovery_url", "http://alice-ccdb.cern.ch:8080/");
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
					logger.log(Level.INFO,
							"Received " + (UDPReceiver.nrPacketsReceived - oldNrPacketsReceived)
									+ " packets per second. \n" + "Total " + UDPReceiver.nrPacketsReceived);
					oldNrPacketsReceived = UDPReceiver.nrPacketsReceived;
				}
			}
		}
	});

	private final Thread incompleteBlobRecovery = new Thread(new Runnable() {
		@Override
		public void run() {
			setName("IncompleteBlobRecovery");

			while (true) {
				for (final Map.Entry<UUID, Blob> entry : inFlight.entrySet()) {
					entry.getKey();
					final Blob blob = entry.getValue();

					if (System.currentTimeMillis() > blob.getLastTouched() + DELTA_T) {
						// ArrayList<Pair> metadataMissingBlocks = blob.getMissingMetadataBlocks();
						// TODO: Metadata recovery

						final ArrayList<Pair> payloadMissingBlocks = blob.getMissingPayloadBlocks();
						if (payloadMissingBlocks == null) {
							// Recover the entire Blob
							try {
								final URL url = new URL(recoveryBaseURL + blob.getUuid().toString());
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

							try {
								final URL url = new URL(recoveryBaseURL + blob.getUuid().toString());
								final HttpURLConnection con = (HttpURLConnection) url.openConnection();
								con.setRequestProperty("Range", "bytes=" + ranges);
								con.setRequestMethod("GET");

								con.setConnectTimeout(5000); // server should be fast (< 5 sec)
								con.setReadTimeout(5000);

								final int status = con.getResponseCode();
								if (status == HttpServletResponse.SC_PARTIAL_CONTENT) {
									try (BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()))) {
										String inputLine;
										final StringBuffer content = new StringBuffer();
										while ((inputLine = in.readLine()) != null) {
											content.append(inputLine);
										}

										// blob.setPayload(System.arraycopy(blob.getPayload(), ));
										// Parse de response and get only the byte ranges without
										// "--THIS_STRING_SEPARATES_5f8a3c40-8e7a-11e9-8e66-112233445566"

										// blob.addByteRange(content.toString().getBytes(), new Pair(0,
										// content.toString().getBytes().length));
										// System.out.println(content.toString());
									}
								}
								else {
									// TODO: retry?
								}

							}
							catch (final Exception e) {
								e.printStackTrace();
							}
						}
					}
				}

				try {
					// Frequent checks
					sleep(100);
				}
				catch (@SuppressWarnings("unused") final InterruptedException e) {
					return;
				}
			}
		}
	});

	private static Comparator<Blob> startTimeComparator = new Comparator<>() {
		@Override
		public int compare(Blob o1, Blob o2) {
			long diff = o1.startTime - o2.startTime;

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
			currentBlobsForKey.add(blob);

			currentBlobsForKey.sort(startTimeComparator);
		}

		logger.log(Level.INFO, "Complete blob with key " + blob.getKey() + " was added to the cache.");
	}

	void processPacket(final FragmentedBlob fragmentedBlob) throws NoSuchAlgorithmException, IOException {
		System.out.println("Fragment payload offset " + fragmentedBlob.getFragmentOffset() + " size " + fragmentedBlob.getblobDataLength());

		final List<Blob> candidates = currentCacheContent.get(fragmentedBlob.getKey());

		if (candidates != null)
			for (final Blob b : candidates)
				if (b.getUuid().equals(fragmentedBlob.getUuid())) {
					// the complete object was already in memory, keep it and ignore retransmissions of other fragments of the same
					return;
				}

		final Blob blob = this.inFlight.computeIfAbsent(fragmentedBlob.getUuid(), k -> new Blob(fragmentedBlob.getKey(), fragmentedBlob.getUuid()));

		blob.addFragmentedBlob(fragmentedBlob);

		if (blob.isComplete()) {
			synchronized (this) {
				nrPacketsReceived++;
			}

			// Remove the blob from inFlight
			if (this.inFlight.remove(blob.getUuid()) == null) {
				// If you get a SMALL_BLOB this statement will be logged
				logger.log(Level.WARNING, "Complete blob " + blob.getUuid() + " was not present in inFlight");
			}

			addToCacheContent(blob);
		}
	}

	private static ExecutorService executorService = null;

	private void queueProcessing(final FragmentedBlob fragmentedBlob) {
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

					System.out.println("Received one fragment on multicast");

					queueProcessing(new FragmentedBlob(buf, packet.getLength()));

					System.out.println("cacheContent: " + currentCacheContent.size());
					System.out.println("inFlight: " + inFlight.size());
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

					System.out.println("Received one fragment on unicast");

					queueProcessing(new FragmentedBlob(buf, packet.getLength()));

					System.out.println("cacheContent: " + currentCacheContent.size());
					System.out.println("inFlight: " + inFlight.size());
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

							if (b.getEndTime() < currentTime) {
								if (logger.isLoggable(Level.INFO))
									logger.log(Level.INFO, "Removing expired object for " + b.getKey() + ": " + b.getUuid() + " (expired " + b.getEndTime() + ")");

								objectIterator.remove();
							}
						}

						if (objects.size() <= 1) {
							// if empty the list will stay around one more iteration, will be collected by the above conditions then
							continue;
						}

						// oldest objects first, should have been the case already but sort nonetheless
						// Collections.sort(objects);

						while (objects.size() > 1) {
							final Blob b = objects.get(0);

							if (currentTime - b.getStartTime() > TTL_FOR_SUPERSEDED_OBJECTS) {
								// more than 5 minutes old and superseded by a newer one, can be removed
								if (logger.isLoggable(Level.INFO))
									logger.log(Level.INFO, "Removing superseded object for " + b.getKey() + ": " + b.getUuid() + " (valid since " + b.getStartTime() + "):\n" + b);

								objects.remove(0);
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
