package ch.alice.o2.ccdb.servlets;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.List;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import ch.alice.o2.ccdb.multicast.Blob;
import ch.alice.o2.ccdb.multicast.UDPReceiver;

/**
 * SQL-backed implementation of CCDB. This servlet only implements GET (and HEAD) for a particular UUID that is known to reside on this server. It should normally not be accessed directly but clients
 * might end up here if the file was not migrated to other physical locations.
 *
 * @author costing
 * @since 2017-10-13
 */
@WebServlet("/download/*")
public class MemoryDownload extends HttpServlet {
	private static final long serialVersionUID = 1L;

	private static final Monitor monitor = MonitorFactory.getMonitor(MemoryDownload.class.getCanonicalName());

	@Override
	protected void doHead(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		try (Timing t = new Timing(monitor, "HEAD_ms")) {
			doGet(request, response, true);
		}
	}

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		try (Timing t = new Timing(monitor, "GET_ms")) {
			doGet(request, response, false);
		}
	}

	private static void doGet(final HttpServletRequest request, final HttpServletResponse response, final boolean head) throws IOException {
		final String pathInfo = request.getPathInfo();

		UUID id = null;

		try {
			id = UUID.fromString(pathInfo.substring(1));
		}
		catch (@SuppressWarnings("unused") final Throwable t) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "The only path information supported is the requested object ID, i.e. '/download/UUID'");
			return;
		}

		Blob match = null;

		for (List<SoftReference<Blob>> candidates : UDPReceiver.currentCacheContent.values()) {
			if (candidates == null || candidates.size() == 0)
				continue;

			for (final SoftReference<Blob> sblob : candidates) {
				final Blob blob = sblob.get();

				if (blob == null)
					continue;

				if (blob.getUuid().equals(id)) {
					match = blob;
					break;
				}
			}

			if (match != null)
				break;
		}

		if (match == null) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, "Object id " + id + " is not in memory");
			return;
		}

		Memory.setHeaders(match, response);

		if (!head) {
			Memory.download(match, request, response);
		}
		else {
			response.setContentLengthLong(match.getPayload().length);
			response.setHeader("Content-Disposition", "inline;filename=\"" + match.getOriginalName() + "\"");
			response.setHeader("Content-Type", match.getMetadataMap().getOrDefault("Content-Type", "application/octet-stream"));
			response.setHeader("Accept-Ranges", "bytes");
			Memory.setMD5Header(match, response);
		}
	}

	@Override
	protected void doDelete(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
		resp.sendError(HttpServletResponse.SC_NOT_IMPLEMENTED, "The DELETE method should use the main entry point instead of /download/, which is reserved for direct read access to the objects");
	}

	@Override
	protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
		resp.sendError(HttpServletResponse.SC_NOT_IMPLEMENTED, "You shouldn't try to create objects via the /download/ servlet, go to / instead");
	}

	@Override
	protected void doPut(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
		resp.sendError(HttpServletResponse.SC_NOT_IMPLEMENTED, "Object manipulation is not available via the /download/ servlet, go to / instead");
	}
}
