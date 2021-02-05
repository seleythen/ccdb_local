/**
 *
 */
package ch.alice.o2.ccdb.servlets;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.shell.commands.JAliEnCOMMander;
import lazyj.DBFunctions;
import lazyj.cache.ExpirationCache;

/**
 * @author costing
 * @since Feb 5, 2021
 */
public class AsyncMulticastQueue {
	private static final Monitor monitor = MonitorFactory.getMonitor(AsyncMulticastQueue.class.getCanonicalName());

	private static ExpirationCache<UUID, UUID> recentlyBroadcastedObjects = new ExpirationCache<>();

	private static BlockingQueue<SQLObject> toMulticastQueue = new LinkedBlockingQueue<>(1000);

	private static BlockingQueue<SQLObject> toStageQueue = new LinkedBlockingQueue<>(1000);

	private static SQLtoUDP sender = SQLtoUDP.getInstance();

	/**
	 * Queue an object to be staged from Grid (if not present locally) and sent by multicast. The operation is refused for repeated calls on the same object ID in less than 30s.
	 * Moreover the sending queue is limited in size to 1000 entries to limit the impact of accidental operations.
	 *
	 * @param obj
	 * @return <code>true</code> if the object was accepted for this operation, <code>false</code> if it was rejected
	 */
	public static boolean queueObject(final SQLObject obj) {
		if (recentlyBroadcastedObjects.get(obj.id) != null)
			return false;

		recentlyBroadcastedObjects.put(obj.id, obj.id, 1000 * 30);

		if (obj.getLocalFile(false) != null) {
			// we have the local file, thus is can be sent by multicast
			if (!SQLBacked.udpSender())
				return false;

			return toMulticastQueue.offer(obj);
		}

		if (SQLBacked.gridBacking())
			return toStageQueue.offer(obj);

		return false;
	}

	private static Thread multicastSenderThread = new Thread("AsyncMulticastQueue.sender") {
		@Override
		public void run() {
			while (true) {
				SQLObject obj;
				try {
					obj = toMulticastQueue.take();
				}
				catch (@SuppressWarnings("unused") final InterruptedException e) {
					return;
				}

				if (obj != null && SQLBacked.udpSender() && sender != null)
					sender.newObject(obj);
			}
		}
	};

	private static Thread stagerThread = new Thread("AsyncMulticastQueue.stager") {
		@Override
		public void run() {
			while (true) {
				SQLObject obj;

				try {
					obj = toStageQueue.take();
				}
				catch (@SuppressWarnings("unused") final InterruptedException e) {
					return;
				}

				if (obj == null)
					continue;

				if (stage(obj))
					try {
						if (SQLBacked.udpSender())
							toMulticastQueue.put(obj);
					}
					catch (@SuppressWarnings("unused") final InterruptedException e) {
						return;
					}
			}
		}
	};

	private static JAliEnCOMMander commander = null;

	/**
	 * @param obj
	 * @return <code>true</code> if the file is ready to be sent, <code>false</code> if any problem
	 */
	private static boolean stage(final SQLObject obj) {
		if (obj == null)
			return false;

		if (obj.getLocalFile(false) != null)
			return true;

		File target = obj.getLocalFile(true);

		if (target == null)
			return false;

		String targetObjectPath = obj.getAddress(Integer.valueOf(-1), null, false).iterator().next();

		if (targetObjectPath == null || !targetObjectPath.startsWith("alien://"))
			return false;

		targetObjectPath = targetObjectPath.substring(8);

		if (commander == null)
			commander = new JAliEnCOMMander(null, null, "CERN", null);

		try (Timing t = new Timing(monitor, "stage_in_ms")) {
			try {
				commander.c_api.downloadFile(targetObjectPath, target);
			}
			catch (@SuppressWarnings("unused") final IOException ioe) {
				return false;
			}
		}

		target = obj.getLocalFile(false);

		if (target != null) {
			// checks have passed, we have the file
			try (DBFunctions db = SQLObject.getDB()) {
				db.query("update ccdb set replicas=replicas || ? where id=? AND NOT ? = ANY(replicas);", false, Integer.valueOf(0), obj.id, Integer.valueOf(0));
			}

			return true;
		}

		return false;
	}

	static {
		if (SQLBacked.udpSender() && sender != null) {
			multicastSenderThread.setDaemon(true);
			multicastSenderThread.start();

			stagerThread.setDaemon(true);
			stagerThread.start();
		}
	}
}
