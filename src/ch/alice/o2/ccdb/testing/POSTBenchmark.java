/**
 *
 */
package ch.alice.o2.ccdb.testing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import alien.monitoring.Timing;
import alien.test.cassandra.tomcat.Options;
import lazyj.Format;
import lazyj.Utils;

/**
 * @author costing
 * @since Jun 4, 2021
 */
public class POSTBenchmark {

	private static AtomicLong requestsMade = new AtomicLong();
	private static AtomicLong nanosTook = new AtomicLong();
	private static AtomicLong errors = new AtomicLong();

	private static AtomicInteger completedThreads = new AtomicInteger();

	private static int iterations = 10000;

	private static Object syncObject = new Object();

	private static byte[] buffer;

	private static final String repository = Options.getOption("repository.url", "http://ccdb-test.cern.ch:8080/");

	private static class RequestThread extends Thread {
		private final URL url;

		/**
		 * @throws MalformedURLException
		 *
		 */
		RequestThread(final int threadNo) throws MalformedURLException {
			url = new URL(repository + "Costin/benchmark/thread_" + threadNo + "/1/10");
		}

		@Override
		public void run() {
			final String boundary = UUID.randomUUID().toString();
			final String boundaryString = "--" + boundary + "\r\n";
			final byte[] finishBoundaryBytes = ("\r\n--" + boundary + "--").getBytes(StandardCharsets.UTF_8);

			final String mpHeader = "Content-Disposition: form-data; name=\"blob\"; filename=\"benchmark\"\r\n" + "Content-Length: " + buffer.length + "\r\n"
					+ "Content-Type: text/plain\r\n" + "\r\n";

			final byte[] multipartHeader = (boundaryString + mpHeader).getBytes(StandardCharsets.UTF_8);

			for (int i = 0; i < iterations; i++) {
				try (Timing t = new Timing()) {
					final HttpURLConnection http = (HttpURLConnection) url.openConnection();
					http.setRequestMethod("POST");
					http.setRequestProperty("Expect", "100-continue");

					http.setDoOutput(true);
					http.setDoInput(true);

					http.setRequestProperty("Content-Type", "multipart/form-data; charset=UTF-8; boundary=" + boundary);

					http.setFixedLengthStreamingMode(multipartHeader.length + buffer.length + finishBoundaryBytes.length);

					try (OutputStream httpOut = http.getOutputStream()) {
						httpOut.write(multipartHeader);

						httpOut.write(buffer);

						httpOut.write(finishBoundaryBytes);

						httpOut.flush();

						String line;

						try (BufferedReader br = new BufferedReader(new InputStreamReader(http.getInputStream()))) {
							while ((line = br.readLine()) != null)
								System.err.println(line);
						}
					}

					requestsMade.incrementAndGet();
					nanosTook.addAndGet(t.getNanos());
				}
				catch (final Exception ioe) {
					ioe.printStackTrace();

					errors.incrementAndGet();
				}
			}

			completedThreads.incrementAndGet();

			synchronized (syncObject) {
				syncObject.notifyAll();
			}
		}

	}

	private static void printProgress(final Timing global, final boolean lastReport) {
		final long req = requestsMade.get();

		if (req == 0)
			return;

		final double millis = nanosTook.get() / 1000000.;
		System.err.println((lastReport ? "In total " : "So far ") + requestsMade + " requests were made in " + global + " for a global rate of "
				+ Format.point(req / global.getSeconds()) + " Hz and an average time per call of " + Format.point(millis / req) + " ms. " + errors.get() + " errors were recorded.");
	}

	private static void truncate() throws IOException {
		System.err.println("Truncating the benchmark paths");

		try (Timing t = new Timing()) {
			Utils.download(repository + "truncate/Costin/benchmark/.*", null);
			System.err.println("    took: " + t);
		}
	}

	/**
	 * Benchmark entry point
	 *
	 * @param args
	 * @throws Exception
	 * @throws MalformedURLException
	 */
	public static void main(final String[] args) throws Exception {
		final int threads = args.length > 0 ? Integer.parseInt(args[0]) : 10;

		if (args.length > 1)
			iterations = Integer.parseInt(args[1]);

		final int length = args.length > 2 ? Integer.parseInt(args[2]) : 1024;

		truncate();

		System.err.println("Starting " + threads + " threads to make " + iterations + " requests to upload objects of " + length + " bytes");

		buffer = new byte[length];

		for (int i = 0; i < length; i++)
			buffer[i] = (byte) ('a' + ThreadLocalRandom.current().nextInt(20));

		final ArrayList<RequestThread> list = new ArrayList<>();

		for (int i = 0; i < threads; i++)
			list.add(new RequestThread(i));

		for (final RequestThread rt : list)
			rt.start();

		try (Timing global = new Timing()) {
			while (completedThreads.get() < threads) {
				printProgress(global, false);

				synchronized (syncObject) {
					try {
						syncObject.wait(10000);
					}
					catch (@SuppressWarnings("unused") final InterruptedException e) {
						break;
					}
				}
			}

			printProgress(global, true);
		}

		truncate();
	}
}
