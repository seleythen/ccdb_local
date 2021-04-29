package ch.alice.o2.ccdb.servlets;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import ch.alice.o2.ccdb.RequestParser;
import ch.alice.o2.ccdb.servlets.formatters.FormatterFactory;
import ch.alice.o2.ccdb.servlets.formatters.SQLFormatter;
import lazyj.DBFunctions;
import lazyj.Utils;

/**
 * SQL-backed implementation of CCDB. This servlet implements browsing of objects in a particular path
 *
 * @author costing
 * @since 2018-04-26
 */
@WebServlet("/browse/*")
public class SQLBrowse extends HttpServlet {
	private static final long serialVersionUID = 1L;

	private static final Monitor monitor = MonitorFactory.getMonitor(SQLBrowse.class.getCanonicalName());

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		try (Timing t = new Timing(monitor, "GET_ms")) {
			// list of objects matching the request
			// URL parameters are:
			// task name / detector name [ / time [ / UUID ] | [ / query string]* ]
			// if time is missing - get the last available time
			// query string example: "quality=2"

			final RequestParser parser = new RequestParser(request, true);

			/*
			 * if (!parser.ok) {
			 * printUsage(request, response);
			 * return;
			 * }
			 */

			final Collection<SQLObject> matchingObjects = SQLObject.getAllMatchingObjects(parser);

			final SQLFormatter formatter = FormatterFactory.getFormatter(request);

			response.setContentType(formatter.getContentType());

			final boolean sizeReport = Utils.stringToBool(request.getParameter("report"), false);

			final boolean prepare = Utils.stringToBool(request.getParameter("prepare"), false);

			formatter.setExtendedReport(sizeReport);

			try (PrintWriter pw = response.getWriter()) {
				formatter.start(pw);

				formatter.header(pw);

				boolean first = true;

				if (matchingObjects != null)
					for (final SQLObject object : matchingObjects) {
						if (first)
							first = false;
						else
							formatter.middle(pw);

						formatter.format(pw, object);

						if (prepare && parser.latestFlag)
							AsyncMulticastQueue.queueObject(object);
					}

				formatter.footer(pw);

				if (!parser.wildcardMatching) {
					// It is not clear which subfolders to list in case of a wildcard matching of objects. As the full hierarchy was included in the search there is no point in showing them, so just
					// skip
					// this section.
					formatter.subfoldersListingHeader(pw);

					long thisFolderCount = 0;
					long thisFolderSize = 0;

					try (DBFunctions db = SQLObject.getDB()) {
						String prefix = "";

						if (parser.path == null || parser.path.length() == 0)
							db.query("select distinct split_part(path,'/',1) from ccdb_paths order by 1;");
						else {
							int cnt = 0;

							for (final char c : parser.path.toCharArray())
								if (c == '/')
									cnt++;

							db.query("select distinct split_part(path,'/',?) from ccdb_paths where path like ? order by 1;", false, Integer.valueOf(cnt + 2), parser.path + "/%");

							prefix = parser.path + "/";
						}

						final StringBuilder suffix = new StringBuilder();

						if (parser.startTimeSet)
							suffix.append('/').append(parser.startTime);

						if (parser.uuidConstraint != null)
							suffix.append('/').append(parser.uuidConstraint);

						for (final Map.Entry<String, String> entry : parser.flagConstraints.entrySet())
							suffix.append('/').append(entry.getKey()).append('=').append(entry.getValue());

						first = true;

						while (db.moveNext()) {
							if (first)
								first = false;
							else
								formatter.middle(pw);

							final String folder = prefix + db.gets(1);

							if (sizeReport) {
								try (DBFunctions db2 = SQLObject.getDB()) {
									db2.query("SELECT object_count, object_size FROM ccdb_stats WHERE pathid=(SELECT pathid FROM ccdb_paths WHERE path=?);", false, folder);

									final long ownCount = db2.getl(1);
									final long ownSize = db2.getl(2);

									db2.query("SELECT sum(object_count), sum(object_size) FROM ccdb_stats WHERE pathid IN (SELECT pathid FROM ccdb_paths WHERE path LIKE ?);", false,
											folder + "/%");

									final long subfoldersCount = db2.getl(1);
									final long subfoldersSize = db2.getl(2);

									formatter.subfoldersListing(pw, folder, folder + suffix, ownCount, ownSize, subfoldersCount, subfoldersSize);
								}
							}
							else
								formatter.subfoldersListing(pw, folder, folder + suffix);
						}

						if (sizeReport) {
							db.query("SELECT object_count, object_size FROM ccdb_stats WHERE pathid=(SELECT pathid FROM ccdb_paths WHERE path=?);", false, parser.path);

							thisFolderCount = db.getl(1);
							thisFolderSize = db.getl(2);
						}
					}

					formatter.subfoldersListingFooter(pw, thisFolderCount, thisFolderSize);
				}

				formatter.end(pw);
			}
		}
	}
}
