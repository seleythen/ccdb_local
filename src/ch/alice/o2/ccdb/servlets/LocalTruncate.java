package ch.alice.o2.ccdb.servlets;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import ch.alice.o2.ccdb.RequestParser;

/**
 * Remove all matching objects in a single operation.
 * TODO: protect or even disable it in production.
 *
 * @author costing
 * @since 2019-06-24
 */
@WebServlet("/truncate/*")
public class LocalTruncate extends HttpServlet {
	private static final long serialVersionUID = 1L;

	private static final Monitor monitor = MonitorFactory.getMonitor(LocalTruncate.class.getCanonicalName());

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		final Set<String> pathsToCheck = new HashSet<>();

		final Timing timing = new Timing();

		try {
			final RequestParser parser = new RequestParser(request, true);

			final Collection<LocalObjectWithVersion> matchingObjects = LocalBrowse.getAllMatchingObjects(parser);

			if (matchingObjects != null && matchingObjects.size() > 0) {
				for (final LocalObjectWithVersion object : matchingObjects) {
					if (!object.referenceFile.delete()) {
						response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Could not delete the underlying file: " + object.referenceFile.getAbsolutePath());
						return;
					}

					final File fProperties = new File(object.referenceFile.getPath() + ".properties");
					fProperties.delete();

					final String path = object.getPath();

					pathsToCheck.add(path.substring(0, path.lastIndexOf('/')));
				}

				response.setHeader("Deleted", matchingObjects.size() + " objects");

				response.sendError(HttpServletResponse.SC_NO_CONTENT);
			}
			else
				response.sendError(HttpServletResponse.SC_NOT_MODIFIED);
		}
		finally {
			monitor.addMeasurement("TRUNCATE_ms", timing);

			for (String pathToCheck : pathsToCheck) {
				while (pathToCheck != null) {
					final File f = new File(Local.basePath + "/" + pathToCheck);

					System.err.println("Checking " + f.getAbsolutePath());

					if (f.exists() && f.isDirectory()) {
						if (!f.delete()) {
							System.err.println("Cannot delete");
							// non empty, don't try to go up the hierarchy
							break;
						}
					}
					else {
						System.err.println("Not met");
						// already gone or not a CCDB folder
						break;
					}

					final int idx = pathToCheck.lastIndexOf('/');

					if (idx > 0)
						pathToCheck = pathToCheck.substring(0, idx);
					else
						pathToCheck = null;
				}
			}
		}
	}
}
