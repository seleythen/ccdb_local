package ch.alice.o2.ccdb.servlets;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Physical removal of files is expensive so don't make the client wait until it happens but instead return control immediately and do the physical removal asynchronously
 *
 * @author costing
 * @since 2018-06-08
 */
public class AsyncPhysicalRemovalThread extends Thread {
	private AsyncPhysicalRemovalThread() {
		// singleton
	}

	@Override
	public void run() {
		while (true) {
			SQLObject object;
			try {
				object = asyncPhysicalRemovalQueue.take();

				if (object != null)
					for (final SQLNotifier notifier : SQLBacked.getNotifiers())
						notifier.deletedObject(object);
			}
			catch (final InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private final BlockingQueue<SQLObject> asyncPhysicalRemovalQueue = new LinkedBlockingQueue<>();

	private static AsyncPhysicalRemovalThread instance = null;

	static synchronized AsyncPhysicalRemovalThread getInstance() {
		if (instance == null) {
			instance = new AsyncPhysicalRemovalThread();
			instance.start();
		}

		return instance;
	}

	static void queueDeletion(final SQLObject object) {
		getInstance().asyncPhysicalRemovalQueue.offer(object);
	}
}
