package ch.alice.o2.ccdb.servlets;

import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Physical removal of files is expensive so don't make the client wait until it happens but instead return control immediately and do the physical removal asynchronously
 * 
 * @author costing
 * @since 2018-06-08
 */
public class AsyncPhyisicalRemovalThread extends Thread {
	private AsyncPhyisicalRemovalThread() {
		// singleton
	}

	@Override
	public void run() {
		while (true) {
			SQLObject object;
			try {
				object = asyncPhysicalRemovalQueue.take();

				if (object != null)
					deleteReplicas(object);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private final BlockingQueue<SQLObject> asyncPhysicalRemovalQueue = new LinkedBlockingQueue<>();

	private static AsyncPhyisicalRemovalThread instance = null;

	static synchronized AsyncPhyisicalRemovalThread getInstance() {
		if (instance == null) {
			instance = new AsyncPhyisicalRemovalThread();
			instance.start();
		}

		return instance;
	}

	static void queueDeletion(final SQLObject object) {
		getInstance().asyncPhysicalRemovalQueue.offer(object);
	}

	static void deleteReplicas(final SQLObject object) {
		for (final Integer replica : object.replicas)
			if (replica.intValue() == 0) {
				// local file
				final File f = object.getLocalFile(false);

				if (f != null)
					f.delete();
			}
			else {
				// TODO: remote removal
			}
	}
}
