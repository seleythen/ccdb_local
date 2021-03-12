package ch.alice.o2.ccdb.testing;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import ch.alice.o2.ccdb.servlets.SQLObject;
import lazyj.DBFunctions;
import lazyj.Format;

/**
 * Insert N objects to database
 *
 * @author rmucha
 * @since 2021-01-09
 */

public class SQLInsertBulk {
	/**
	 * @param args
	 * @throws InterruptedException
	 */
	private static String[] pathsDB1 = { "A", "B", "C", "D", "E" };
	private static String[] pathsDB2 = { "A", "B", "C", "X", "E" };

	public static void main(final String[] args) throws InterruptedException {
		final long noOfObjects = args.length >= 1 ? Long.parseLong(args[0]) : 100000;

		final long noThreads = args.length >= 2 ? Long.parseLong(args[1]) : 10;

		final long dbNumber = args.length >= 3 ? Long.parseLong(args[2]) : 1;

		System.err.println("Executing vacuum");

		try (DBFunctions db = SQLObject.getDB()) {
			db.query("VACUUM FULL ANALYZE ccdb;");
		}

		final List<Thread> threads = new ArrayList<>();

		System.err.println(
				"Inserting " + (noOfObjects * noThreads) + " new objects in the database on " + noThreads + " threads");

		final long startTime = System.currentTimeMillis();

		final Random r = new Random(System.currentTimeMillis());

		for (long thread = 0; thread < noThreads; thread++) {
			final long localThread = thread;

			final Thread t = new Thread() {
				@Override
				public void run() {
					String[] paths = dbNumber == 1 ? pathsDB1 : pathsDB2;
					int pathsSize = paths.length;
					for (long i = 0; i < noOfObjects; i++) {
						String path = paths[r.nextInt(pathsSize)];
						final SQLObject obj = new SQLObject(path);

						obj.validFrom = ( i + localThread * noOfObjects) * 160;
						obj.validUntil = obj.validFrom + 600000;

						obj.fileName = "some_new_detector_object.root";
						obj.contentType = "application/octet-stream";
						obj.uploadedFrom = "127.0.0.1";
						obj.size = localThread * noOfObjects + i;
						obj.md5 = "7e8fbee4f76f7079ec87bdc83d7d5538";

						obj.replicas.add(Integer.valueOf(1));

						obj.save(null);
					}
				}
			};

			t.start();

			threads.add(t);
		}

		for (final Thread t : threads)
			t.join();

		if (noOfObjects * noThreads > 0) {
			// print insert statistics
			System.err.println(noOfObjects * noThreads + " created in "
					+ Format.toInterval(System.currentTimeMillis() - startTime) + " ("
					+ (double) (System.currentTimeMillis() - startTime) / (noOfObjects * noThreads) + " ms/object)");

			System.err.println((noOfObjects * noThreads * 1000.) / (System.currentTimeMillis() - startTime) + " Hz");
		}
	}
}
