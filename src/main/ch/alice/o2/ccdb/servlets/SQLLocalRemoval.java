package ch.alice.o2.ccdb.servlets;

import java.io.File;

/**
 * Remove binary blobs from the local repository
 *
 * @author costing
 * @since 2019-10-17
 */
public class SQLLocalRemoval implements SQLNotifier {
	private SQLLocalRemoval() {
		// singleton
	}

	private static SQLLocalRemoval instance = null;

	/**
	 * @return singleton
	 */
	static synchronized SQLLocalRemoval getInstance() {
		if (instance == null) {
			instance = new SQLLocalRemoval();
		}

		return instance;
	}

	@Override
	public void newObject(final SQLObject object) {
		// do nothing
	}

	private static final Integer localReplica = Integer.valueOf(0);

	@Override
	public void deletedObject(final SQLObject object) {
		if (object.replicas.contains(localReplica)) {
			final File f = object.getLocalFile(false);

			if (f != null)
				f.delete();

			object.replicas.remove(localReplica);
		}
	}

	@Override
	public void updatedObject(final SQLObject object) {
		// nothing to do on update, the underlying backend doesn't need to know of metadata changes
	}

	@Override
	public String toString() {
		return "SQLLocalRemoval";
	}
}
