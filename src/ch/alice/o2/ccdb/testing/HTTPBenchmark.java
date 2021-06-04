/**
 *
 */
package ch.alice.o2.ccdb.testing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import alien.monitoring.Timing;
import lazyj.Format;

/**
 * @author costing
 * @since Jun 4, 2021
 */
public class HTTPBenchmark {

	private static AtomicLong requestsMade = new AtomicLong();
	private static AtomicLong nanosTook = new AtomicLong();
	private static AtomicLong errors = new AtomicLong();

	private static AtomicInteger completedThreads = new AtomicInteger();

	private static int iterations = 10000;

	private static Object syncObject = new Object();

	private static class RequestThread extends Thread {

		@Override
		public void run() {
			for (int i = 0; i < iterations; i++) {
				try (Timing t = new Timing()) {
					try {
						lazyj.Utils.download("http://ccdb-test.cern.ch:8080/", null);

						requestsMade.incrementAndGet();
						nanosTook.addAndGet(t.getNanos());
					}
					catch (@SuppressWarnings("unused") final IOException ioe) {
						errors.incrementAndGet();
					}
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
				+ Format.point(req / global.getSeconds()) + " Hz and an average time per call of " + Format.point(millis / req) + " ms");
	}

	/**
	 * Benchmark entry point
	 * 
	 * @param args
	 */
	public static void main(final String[] args) {
		final int threads = args.length > 0 ? Integer.parseInt(args[0]) : 100;

		if (args.length > 1)
			iterations = Integer.parseInt(args[1]);

		System.err.println("Making " + iterations + " requests from " + threads + " threads");

		final ArrayList<RequestThread> list = new ArrayList<>();

		for (int i = 0; i < threads; i++)
			list.add(new RequestThread());

		for (final RequestThread rt : list)
			rt.start();

		try (Timing global = new Timing()) {
			while (completedThreads.get() < threads) {
				printProgress(global, false);

				synchronized (syncObject) {
					try {
						syncObject.wait(10000);
					}
					catch (@SuppressWarnings("unused") InterruptedException e) {
						break;
					}
				}
			}

			printProgress(global, true);
		}
	}
}
