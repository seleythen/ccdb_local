package ch.alice.o2.ccdb.servlets;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import ch.alice.o2.ccdb.RequestParser;
import ch.alice.o2.ccdb.servlets.formatters.HTMLFormatter;
import ch.alice.o2.ccdb.servlets.formatters.JSONFormatter;
import ch.alice.o2.ccdb.servlets.formatters.SQLFormatter;
import ch.alice.o2.ccdb.servlets.formatters.TextFormatter;
import ch.alice.o2.ccdb.servlets.formatters.XMLFormatter;
import lazyj.DBFunctions;
import lazyj.Format;

/**
 * SQL-backed implementation of CCDB. This servlet implements browsing of objects in a particular path
 *
 * @author costing
 * @since 2018-04-26
 */
@WebServlet("/browse/*")
public class SQLBrowse extends HttpServlet {
	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
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

		String sContentType;
		SQLFormatter formatter = null;

		String sAccept = request.getParameter("Accept");

		if ((sAccept == null) || (sAccept.length() == 0))
			sAccept = request.getHeader("Accept");

		if ((sAccept == null || sAccept.length() == 0))
			sAccept = "text/plain";

		sAccept = sAccept.toLowerCase();

		if ((sAccept.indexOf("application/json") >= 0) || (sAccept.indexOf("text/json") >= 0)) {
			sContentType = "application/json";
			formatter = new JSONFormatter();
		}
		else
			if (sAccept.indexOf("text/html") >= 0) {
				sContentType = "text/html";
				formatter = new HTMLFormatter();
			}
			else
				if (sAccept.indexOf("text/xml") >= 0) {
					sContentType = "text/xml";
					formatter = new XMLFormatter();
				}
				else {
					sContentType = "text/plain";
					formatter = new TextFormatter();
				}

		response.setContentType(sContentType);

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
				}

			formatter.footer(pw);

			formatter.subfoldersListingHeader(pw);

			try (DBFunctions db = SQLObject.getDB()) {
				String prefix = "";

				if (parser.path == null || parser.path.length() == 0)
					db.query("select distinct split_part(path,'/',1) from ccdb_paths order by 1;");
				else {
					int cnt = 0;

					for (final char c : parser.path.toCharArray())
						if (c == '/')
							cnt++;

					db.query("select distinct split_part(path,'/'," + (cnt + 2) + ") from ccdb_paths where path like '" + Format.escSQL(parser.path) + "/%' order by 1;");

					prefix = parser.path + "/";
				}

				first = true;

				while (db.moveNext()) {
					if (first)
						first = false;
					else
						formatter.middle(pw);

					formatter.subfoldersListing(pw, prefix + db.gets(1));
				}
			}

			formatter.subfoldersListingFooter(pw);

			formatter.end(pw);
		}
	}
}
