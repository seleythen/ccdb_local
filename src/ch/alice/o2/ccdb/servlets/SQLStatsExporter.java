package ch.alice.o2.ccdb.servlets;

import java.util.Vector;

import alien.monitoring.MonitoringObject;
import lazyj.DBFunctions;
import lazyj.Format;

/**
 * Publish monitoring information about the metadata repository
 *
 * @author costing
 * @since 2019-05-24
 */
public class SQLStatsExporter implements MonitoringObject {
	private final String path;
	private final int depth;

	/**
	 * Indicate which path to create the stats for. If <code>null</code> then the first level of directories is exported. When non-null, all sub-folders of that path are exported.
	 * 
	 * @param path
	 */
	public SQLStatsExporter(final String path) {
		this.path = path;

		if (path != null) {
			int slashesCount = 0;

			for (char c : path.toCharArray())
				if (c == '/')
					slashesCount++;

			depth = slashesCount + 2;
		}
		else
			depth = 0;
	}

	@Override
	public void fillValues(final Vector<String> paramNames, final Vector<Object> paramValues) {
		try (DBFunctions db = SQLObject.getDB()) {
			if (path != null)
				db.query("SELECT split_part(path,'/'," + depth + "), sum(object_count), sum(object_size) FROM ccdb_paths INNER JOIN ccdb_stats USING(pathid) WHERE path LIKE '"
						+ Format.escSQL(path) + "/%' GROUP BY 1;");
			else
				db.query("SELECT split_part(path,'/',1), sum(object_count), sum(object_size) FROM ccdb_paths INNER JOIN ccdb_stats USING(pathid) GROUP BY 1;");

			long totalCount = 0;
			long totalSize = 0;

			while (db.moveNext()) {
				final String folder = db.gets(1);
				final long pathObjectCount = db.getl(2);
				final long pathObjectSize = db.getl(3);

				totalCount += pathObjectCount;
				totalSize += pathObjectSize;

				paramNames.add(folder + "_count");
				paramNames.add(folder + "_size");

				paramValues.add(Double.valueOf(pathObjectCount));
				paramValues.add(Double.valueOf(pathObjectSize));
			}

			paramNames.add("_TOTALS__count");
			paramNames.add("_TOTALS__size");

			paramValues.add(Double.valueOf(totalCount));
			paramValues.add(Double.valueOf(totalSize));
		}
	}
}
