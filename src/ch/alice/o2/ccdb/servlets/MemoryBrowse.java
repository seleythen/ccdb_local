package ch.alice.o2.ccdb.servlets;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import ch.alice.o2.ccdb.RequestParser;
import ch.alice.o2.ccdb.multicast.Blob;
import ch.alice.o2.ccdb.multicast.UDPReceiver;
import ch.alice.o2.ccdb.servlets.formatters.HTMLFormatter;
import ch.alice.o2.ccdb.servlets.formatters.JSONFormatter;
import ch.alice.o2.ccdb.servlets.formatters.SQLFormatter;
import ch.alice.o2.ccdb.servlets.formatters.TextFormatter;
import ch.alice.o2.ccdb.servlets.formatters.XMLFormatter;
import lazyj.Format;

/**
 * In-memory implementation of CCDB. This servlet implements browsing of
 * objects in a particular path
 *
 * @author costing
 * @since 2018-04-26
 */
@WebServlet("/browse/*")
public class MemoryBrowse extends HttpServlet {
	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		// list of objects matching the request
		// URL parameters are:
		// task name / detector name [ / time [ / UUID ] | [ / query string]* ]
		// if time is missing - get the last available time
		// query string example: "quality=2"

		final RequestParser parser = new RequestParser(request, true);

		final Collection<Blob> matchingObjects = getAllMatchingObjects(parser);

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
				for (final Blob object : matchingObjects) {
					if (first)
						first = false;
					else
						formatter.middle(pw);

					formatter.format(pw, object);
				}

			formatter.footer(pw);

			if (!parser.wildcardMatching) {
				// It is not clear which subfolders to list in case of a wildcard matching of
				// objects. As the full hierarchy was included in the search there is no point
				// in showing them, so just skip
				// this section.
				formatter.subfoldersListingHeader(pw);

				String suffix = "";

				if (parser.startTimeSet)
					suffix += "/" + parser.startTime;

				if (parser.uuidConstraint != null)
					suffix += "/" + parser.uuidConstraint;

				for (final Map.Entry<String, String> entry : parser.flagConstraints.entrySet())
					suffix += "/" + entry.getKey() + "=" + entry.getValue();

				// TODO: search for all keys that have as prefix the current parser.path

				formatter.subfoldersListingFooter(pw, 0, 0);
			}

			formatter.end(pw);
		}
	}

	/**
	 * @param parser
	 * @return all matching objects given the parser constraints
	 */
	public static final Collection<Blob> getAllMatchingObjects(final RequestParser parser) {
		final List<Blob> matchingObjects = new ArrayList<>();

		String pathFilter = parser.path != null ? parser.path : "";

		Pattern pFilter = null;

		if (pathFilter.indexOf('*') >= 0 || pathFilter.indexOf('%') >= 0) {
			parser.wildcardMatching = true;

			pathFilter = Format.replace(pathFilter, "%", ".*");

			pFilter = Pattern.compile("^" + pathFilter + ".*$");
		}
		else {
			if (pathFilter.endsWith("/"))
				pathFilter = pathFilter.substring(0, pathFilter.length() - 1);
		}

		for (Map.Entry<String, List<Blob>> entry : UDPReceiver.currentCacheContent.entrySet()) {
			String path = entry.getKey();

			if ((pFilter == null && path.equals(pathFilter)) || (pFilter != null && pFilter.matcher(path).matches())) {
				Blob bBest = null;

				for (Blob b : entry.getValue()) {
					if (Memory.blobMatchesParser(b, parser)) {
						if (parser.latestFlag) {
							if (bBest == null || b.compareTo(bBest) < 0)
								bBest = b;
						}
						else
							matchingObjects.add(b);
					}
					else
						System.err.println("Ignoring: " + b);
				}

				if (bBest != null)
					matchingObjects.add(bBest);
			}
		}

		return matchingObjects;
	}
}
