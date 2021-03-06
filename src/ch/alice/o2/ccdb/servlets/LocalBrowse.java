package ch.alice.o2.ccdb.servlets;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import lazyj.Format;

/**
 * SQL-backed implementation of CCDB. This servlet implements browsing of objects in a particular path
 *
 * @author costing
 * @since 2018-04-26
 */
@WebServlet("/browse/*")
public class LocalBrowse extends HttpServlet {
	private static final long serialVersionUID = 1L;

	private static final Monitor monitor = MonitorFactory.getMonitor(LocalBrowse.class.getCanonicalName());

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		try (Timing t = new Timing(monitor, "GET_ms")) {
			// list of objects matching the request
			// URL parameters are:
			// task name / detector name [ / time [ / UUID ] | [ / query string]* ]
			// if time is missing - get the last available time
			// query string example: "quality=2"

			final RequestParser parser = new RequestParser(request, true);
			
			CCDBUtils.disableCaching(response);

			// The "ok" check restricts browsing of root directory, which is needed by QCG (as per O2-1859)
			// if (!parser.ok) {
			// printUsage(request, response);
			// return;
			// }

			final List<LocalObjectWithVersion> matchingObjects = getAllMatchingObjects(parser);

			Collections.sort(matchingObjects, Comparator.comparing(LocalObjectWithVersion::getPath));

			final SQLFormatter formatter = FormatterFactory.getFormatter(request);

			response.setContentType(formatter.getContentType());

			try (PrintWriter pw = response.getWriter()) {
				formatter.start(pw);

				formatter.header(pw);

				boolean first = true;

				for (final LocalObjectWithVersion object : matchingObjects) {
					if (first)
						first = false;
					else
						formatter.middle(pw);

					formatter.format(pw, object);
				}

				formatter.footer(pw);

				if (!parser.wildcardMatching) {
					// It is not clear which subfolders to list in case of a wildcard matching of objects. As the full hierarchy was included in the search there is no point in showing them, so just
					// skip this section.
					formatter.subfoldersListingHeader(pw);

					final String prefix = Local.basePath + "/" + parser.path;

					final StringBuilder suffix = new StringBuilder();

					if (parser.startTimeSet)
						suffix.append('/').append(parser.startTime);

					if (parser.uuidConstraint != null)
						suffix.append('/').append(parser.uuidConstraint);

					for (final Map.Entry<String, String> entry : parser.flagConstraints.entrySet())
						suffix.append('/').append(entry.getKey()).append('=').append(entry.getValue());

					final File fBaseDir = new File(prefix);

					final File[] baseDirListing = fBaseDir.listFiles((f) -> f.isDirectory());

					first = true;

					if (baseDirListing != null)
						for (final File fSubdir : baseDirListing) {
							try {
								Long.parseLong(fSubdir.getName());
							}
							catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
								if (first)
									first = false;
								else
									formatter.middle(pw);

								final String pathPrefix = parser.path.length() > 0 ? parser.path + "/" : "";

								formatter.subfoldersListing(pw, pathPrefix + fSubdir.getName(), pathPrefix + fSubdir.getName() + suffix);
							}
						}

					formatter.subfoldersListingFooter(pw, 0, 0);
				}

				formatter.end(pw);
			}
		}
	}

	/**
	 * @param parser
	 * @return all matching objects given the parser constraints
	 */
	public static final List<LocalObjectWithVersion> getAllMatchingObjects(final RequestParser parser) {
		if (parser.path == null)
			parser.path = "";

		final int idxStar = parser.path.indexOf('*');
		final int idxPercent = parser.path.indexOf('%');

		final File fBaseDir;

		final Pattern matchingPattern;

		if (idxStar >= 0 || idxPercent >= 0) {
			parser.wildcardMatching = true;

			final int idxFirst = idxStar >= 0 && idxPercent >= 0 ? Math.min(idxStar, idxPercent) : Math.max(idxStar, idxPercent);

			final int idxLastSlash = parser.path.lastIndexOf('/', idxFirst);

			String fixedPath = Local.basePath;

			String pattern = parser.path;

			if (idxLastSlash >= 0) {
				fixedPath += "/" + parser.path.substring(0, idxLastSlash);
				pattern = parser.path.substring(idxLastSlash + 1);
			}

			pattern = Format.replace(pattern, "*", "[^/]*");
			pattern = Format.replace(pattern, "%", "[^/]*");

			pattern += "/.*";

			matchingPattern = Pattern.compile(fixedPath + "/" + pattern);

			fBaseDir = new File(fixedPath);
		}
		else {
			fBaseDir = new File(Local.basePath + "/" + parser.path);

			if (parser.startTime > 0 && parser.uuidConstraint != null) {
				// is this the full path to a file? if so then download it

				final File toDownload = new File(fBaseDir, parser.startTime + "/" + parser.uuidConstraint.toString());

				if (toDownload.exists() && toDownload.isFile())
					return Arrays.asList(new LocalObjectWithVersion(parser.startTime, toDownload));

				// a particular object was requested but it doesn't exist
				return Collections.emptyList();
			}

			matchingPattern = null;
		}

		final List<LocalObjectWithVersion> ret = new ArrayList<>();

		recursiveMatching(parser, ret, fBaseDir, matchingPattern);

		if (parser.browseLimit > 0 && parser.browseLimit < ret.size()) {
			// return at most Browse-Limit objects, the most recent ones from any matching path
			Collections.sort(ret);
			return ret.subList(0, parser.browseLimit);
		}

		return ret;
	}

	private static void recursiveMatching(final RequestParser parser, final Collection<LocalObjectWithVersion> ret, final File fBaseDir, final Pattern matchingPattern) {
		LocalObjectWithVersion mostRecent = null;

		final File[] baseDirListing = fBaseDir.listFiles((f) -> f.isDirectory());

		if (baseDirListing == null)
			return;

		for (final File fInterval : baseDirListing)
			try {
				final long lValidityStart = Long.parseLong(fInterval.getName());

				if (matchingPattern != null || (parser.startTimeSet && lValidityStart > parser.startTime))
					continue;

				final File[] intervalFileList = fInterval.listFiles((f) -> f.isFile() && !f.getName().contains("."));

				if (intervalFileList == null)
					continue;

				for (final File f : intervalFileList) {
					final LocalObjectWithVersion owv = new LocalObjectWithVersion(lValidityStart, f);

					if ((!parser.startTimeSet || owv.covers(parser.startTime)) && (parser.notAfter <= 0 || owv.getCreateTime() <= parser.notAfter)
							&& (parser.notBefore <= 0 || owv.getCreateTime() >= parser.notBefore) && owv.matches(parser.flagConstraints)) {
						if (parser.latestFlag) {
							if ((mostRecent == null) || (owv.compareTo(mostRecent) < 0))
								mostRecent = owv;
						}
						else
							ret.add(owv);
					}
				}
			}
			catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
				// When the subfolder is not a number then it can be another level of objects, to be inspected as well

				if (parser.wildcardMatching) {
					if (matchingPattern != null) {
						final Matcher m = matchingPattern.matcher(fInterval.getAbsolutePath() + "/");

						if (m.matches()) {
							// full pattern match, from here on we can list files in the subfolders
							recursiveMatching(parser, ret, fInterval, null);
							continue;
						}
					}

					recursiveMatching(parser, ret, fInterval, matchingPattern);
				}
			}

		if (mostRecent != null)
			ret.add(mostRecent);
	}
}
