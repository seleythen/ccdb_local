package ch.alice.o2.ccdb.servlets;

import static ch.alice.o2.ccdb.servlets.ServletHelper.printUsage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.SoftReference;
import java.net.InetAddress;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

import alien.catalogue.GUIDUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.test.cassandra.tomcat.Options;
import ch.alice.o2.ccdb.RequestParser;
import ch.alice.o2.ccdb.UUIDTools;
import ch.alice.o2.ccdb.multicast.Blob;
import ch.alice.o2.ccdb.multicast.UDPReceiver;
import ch.alice.o2.ccdb.multicast.Utils;

/**
 * Prototype implementation of QC repository in memory. Target is the EPN & FLP farm
 * where calibration data should only stay in memory
 *
 * @author costing
 * @since 2019-11-01
 */
@WebServlet("/*")
@MultipartConfig
public class Memory extends HttpServlet {
	private static final long serialVersionUID = 1L;

	private static final Monitor monitor = MonitorFactory.getMonitor(Memory.class.getCanonicalName());

	private static final boolean REDIRECT_TO_UPSTREAM;

	private static final String UPSTREAM_URL;

	static {
		final String recoveryURL = Options.getOption("udp_receiver.recovery_url", "http://alice-ccdb.cern.ch:8080/");

		UPSTREAM_URL = Options.getOption("memory.redirect_changes_url", recoveryURL);

		if (Options.getIntOption("memory.redirect_changes", 1) > 0) {
			if (UPSTREAM_URL != null && UPSTREAM_URL.length() > 0) {
				System.err.println("Memory: redirecting all changes to " + UPSTREAM_URL);
				REDIRECT_TO_UPSTREAM = true;
			}
			else {
				System.err.println("Memory: can't redirect changes anywhere, memory.redirect_changes_url is not set");
				REDIRECT_TO_UPSTREAM = false;
			}
		}
		else {
			System.err.println("Memory: direct uploads are accepted, clients will not be redirected upstream");
			REDIRECT_TO_UPSTREAM = false;
		}
	}

	private static String getURLPrefix(final HttpServletRequest request) {
		return request.getContextPath() + request.getServletPath();
	}

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
		// list of objects matching the request
		// URL parameters are:
		// task name / detector name [ / time [ / UUID ] | [ / query string]* ]
		// if time is missing - get the last available time
		// query string example: "quality=2"

		final RequestParser parser = new RequestParser(request);

		if (!parser.ok) {
			printUsage(request, response);
			return;
		}

		final Blob matchingObject = getMatchingObject(parser);

		if (matchingObject == null) {
			monitor.incrementCacheMisses("memcache");

			// nothing matches, or the best matching object is incomplete

			if (REDIRECT_TO_UPSTREAM) {
				response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
				response.setHeader("Location", UPSTREAM_URL + request.getPathInfo());
			}
			else
				response.sendError(HttpServletResponse.SC_NOT_FOUND, "No matching objects found");

			return;
		}

		monitor.incrementCacheHits("memcache");

		if (parser.cachedValue != null && matchingObject.getUuid().toString().equalsIgnoreCase(parser.cachedValue.toString())) {
			response.sendError(HttpServletResponse.SC_NOT_MODIFIED);
			return;
		}

		if (parser.uuidConstraint != null) {
			// download this explicitly requested file, unless HEAD was indicated in which
			// case no content should be returned to the client

			setHeaders(matchingObject, response);

			if (!head) {
				download(matchingObject, request, response);
			}
			else {
				response.setContentLengthLong(matchingObject.getPayload().length);
				response.setHeader("Content-Disposition", "inline;filename=\"" + matchingObject.getOriginalName() + "\"");
				response.setHeader("Content-Type", matchingObject.getMetadataMap().getOrDefault("Content-Type", "application/octet-stream"));
				response.setHeader("Accept-Ranges", "bytes");
				setMD5Header(matchingObject, response);
			}

			return;
		}

		setHeaders(matchingObject, response);

		response.sendRedirect(getURLPrefix(request) + matchingObject.getStartTime() + "/" + matchingObject.getUuid().toString());
	}

	/**
	 * Set the HTTP headers common for both GET and HEAD requests, for a given object
	 * 
	 * @param obj
	 * @param response
	 */
	static void setHeaders(final Blob obj, final HttpServletResponse response) {
		response.setDateHeader("Date", System.currentTimeMillis());

		try {
			response.setDateHeader("Last-Modified", Long.parseLong(obj.getProperty("Last-Modified")));
		}
		catch (@SuppressWarnings("unused") NullPointerException | NumberFormatException ignore) {
			response.setDateHeader("Last-Modified", (obj.getCreateTime()));
		}

		response.setHeader("ETag", '"' + obj.getUuid().toString() + '"');

		final Map<String, String> metadata = obj.getMetadataMap();

		for (final Map.Entry<String, String> entry : metadata.entrySet()) {
			final String key = entry.getKey();

			if (key.equals("Last-Modified") || key.equals("Date") || key.equals("ETag"))
				continue;

			response.setHeader(entry.getKey(), entry.getValue());
		}
	}

	/**
	 * Set the MD5 checksum header only. For HEAD requests the assumption is that the entire content would be served and this is the checksum
	 * 
	 * @param obj
	 * @param response
	 */
	static void setMD5Header(final Blob obj, final HttpServletResponse response) {
		final String md5 = obj.getMD5();

		if (md5 != null && !md5.isEmpty())
			response.setHeader("Content-MD5", md5);
	}

	/**
	 * Download the content of an object from memory
	 * 
	 * @param obj
	 * @param request
	 * @param response
	 * @throws IOException
	 */
	static void download(final Blob obj, final HttpServletRequest request, final HttpServletResponse response) throws IOException {
		final String range = request.getHeader("Range");

		System.err.println("Client " + request.getRemoteAddr() + " requested to download " + obj.getUuid() + ", range: " + range);

		if (range == null || range.trim().isEmpty()) {
			response.setHeader("Accept-Ranges", "bytes");
			response.setContentLengthLong(obj.getPayload().length);
			response.setHeader("Content-Disposition", "inline;filename=\"" + obj.getOriginalName() + "\"");
			response.setHeader("Content-Type", obj.getProperty("Content-Type", "application/octet-stream"));
			setMD5Header(obj, response);

			try (OutputStream os = response.getOutputStream()) {
				os.write(obj.getPayload(), 0, obj.getPayload().length);
			}

			return;
		}

		// a Range request was made, serve only the requested bytes

		final long payloadSize = obj.getSize();

		if (!range.startsWith("bytes=")) {
			response.setHeader("Content-Range", "bytes */" + payloadSize);
			response.sendError(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE);
			return;
		}

		final StringTokenizer st = new StringTokenizer(range.substring(6).trim(), ",");

		final List<Map.Entry<Long, Long>> requestedRanges = new ArrayList<>();

		while (st.hasMoreTokens()) {
			final String s = st.nextToken();

			final int idx = s.indexOf('-');

			long start;
			long end;

			if (idx > 0) {
				start = Long.parseLong(s.substring(0, idx));

				if (start >= payloadSize) {
					response.setHeader("Content-Range", "bytes */" + payloadSize);
					response.sendError(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE, "You requested an invalid range, starting beyond the end of the file (" + start + ")");
					return;
				}

				if (idx < (s.length() - 1)) {
					end = Long.parseLong(s.substring(idx + 1));

					if (end >= payloadSize) {
						response.setHeader("Content-Range", "bytes */" + payloadSize);
						response.sendError(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE, "You requested bytes beyond what the file contains (" + end + ")");
						return;
					}
				}
				else
					end = payloadSize - 1;

				if (start > end) {
					response.setHeader("Content-Range", "bytes */" + payloadSize);
					response.sendError(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE,
							"The requested range is wrong, the second value (" + end + ") should be larger than the first (" + start + ")");
					return;
				}
			}
			else
				if (idx == 0) {
					// a single negative value means 'last N bytes'
					start = Long.parseLong(s.substring(idx + 1));

					end = payloadSize - 1;

					start = end - start + 1;

					if (start < 0) {
						response.setHeader("Content-Range", "bytes */" + payloadSize);
						response.sendError(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE,
								"You requested more last bytes (" + s.substring(idx + 1) + ") from the end of the file than the file actually has (" + payloadSize + ")");
						start = 0;
					}
				}
				else {
					start = Long.parseLong(s);

					if (start >= payloadSize) {
						response.setHeader("Content-Range", "bytes */" + payloadSize);
						response.sendError(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE, "You requested an invalid range, starting beyond the end of the file (" + start + ")");
						return;
					}

					end = payloadSize - 1;
				}

			requestedRanges.add(new AbstractMap.SimpleEntry<>(Long.valueOf(start), Long.valueOf(end)));
		}

		if (requestedRanges.size() == 0) {
			response.setHeader("Content-Range", "bytes */" + payloadSize);
			response.sendError(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE);
			return;
		}

		response.setStatus(HttpServletResponse.SC_PARTIAL_CONTENT);

		if (requestedRanges.size() == 1) {
			// A single byte range
			final Map.Entry<Long, Long> theRange = requestedRanges.get(0);
			final long first = theRange.getKey().longValue();
			final long last = theRange.getValue().longValue();

			final long toCopy = last - first + 1;

			response.setContentLengthLong(toCopy);
			response.setHeader("Content-Range", "bytes " + first + "-" + last + "/" + payloadSize);
			response.setHeader("Content-Disposition", "inline;filename=\"" + obj.getOriginalName() + "\"");
			response.setHeader("Content-Type", obj.getProperty("Content-Type", "application/octet-stream"));

			try (OutputStream os = response.getOutputStream()) {
				// TODO: Check if cast from long to int would be a problem
				os.write(obj.getPayload(), (int) first, (int) toCopy);
			}

			return;
		}

		// the content length should be computed based on the ranges and the header
		// sizes. Or just don't send it :)
		final String boundaryString = "THIS_STRING_SEPARATES_" + UUIDTools.generateTimeUUID(System.currentTimeMillis(), null).toString();

		response.setHeader("Content-Type", "multipart/byteranges; boundary=" + boundaryString);

		final ArrayList<String> subHeaders = new ArrayList<>(requestedRanges.size());

		long contentLength = 0;

		for (final Map.Entry<Long, Long> theRange : requestedRanges) {
			final long first = theRange.getKey().longValue();
			final long last = theRange.getValue().longValue();

			final long toCopy = last - first + 1;

			final StringBuilder subHeader = new StringBuilder();

			subHeader.append("\n--").append(boundaryString);
			subHeader.append("\nContent-Type: ").append(obj.getProperty("Content-Type", "application/octet-stream")).append('\n');
			subHeader.append("Content-Range: bytes ").append(first).append("-").append(last).append("/").append(payloadSize).append("\n\n");

			final String sh = subHeader.toString();

			subHeaders.add(sh);

			contentLength += toCopy + sh.length();
		}

		final String documentFooter = "\n--" + boundaryString + "--\n";

		contentLength += documentFooter.length();

		response.setContentLengthLong(contentLength);

		try (OutputStream os = response.getOutputStream()) {
			final Iterator<Map.Entry<Long, Long>> itRange = requestedRanges.iterator();
			final Iterator<String> itSubHeader = subHeaders.iterator();

			while (itRange.hasNext()) {
				final Map.Entry<Long, Long> theRange = itRange.next();
				final String subHeader = itSubHeader.next();

				final long first = theRange.getKey().longValue();
				final long last = theRange.getValue().longValue();

				final long toCopy = last - first + 1;

				os.write(subHeader.getBytes());

				// TODO: Check if cast from long to int would be a problem
				os.write(obj.getPayload(), (int) first, (int) toCopy);
			}
			os.write(documentFooter.getBytes());
		}

	}

	@Override
	protected void doPost(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		try (Timing t = new Timing(monitor, "POST_ms")) {
			if (REDIRECT_TO_UPSTREAM && request.getHeader("Force-Upload") == null) {
				response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
				response.setHeader("Location", UPSTREAM_URL + request.getPathInfo());

				return;
			}

			// create the given object and return the unique identifier to it
			// URL parameters are:
			// task name / detector name / start time [/ end time] [ / flag ]*
			// mime-encoded blob is the value to be stored
			// if end time is missing then it will be set to the same value as start time
			// flags are in the form "key=value"

			final RequestParser parser = new RequestParser(request);

			if (!parser.ok) {
				printUsage(request, response);
				return;
			}

			final Collection<Part> parts = request.getParts();

			if (parts.size() == 0) {
				response.sendError(HttpServletResponse.SC_BAD_REQUEST, "POST request doesn't contain the data to upload");
				return;
			}

			final long newObjectTime = System.currentTimeMillis();

			byte[] remoteAddress = null;

			try {
				final InetAddress ia = InetAddress.getByName(request.getRemoteAddr());

				remoteAddress = ia.getAddress();
			}
			catch (@SuppressWarnings("unused") final Throwable t1) {
				// ignore
			}

			final UUID targetUUID = parser.uuidConstraint != null ? parser.uuidConstraint : UUIDTools.generateTimeUUID(newObjectTime, remoteAddress);

			final Part part = parts.iterator().next();

			final byte[] metadata = new byte[0];

			final byte[] payload;
			try (InputStream is = part.getInputStream()) {
				payload = new byte[is.available()];
				is.read(payload);
			}

			final String blobKey = parser.path;
			Blob newBlob;

			try {
				newBlob = new Blob(metadata, payload, blobKey, targetUUID);

				newBlob.startTime = parser.startTime;
				newBlob.endTime = parser.endTime;

				if (parser.flagConstraints != null)
					newBlob.setMetadata(Utils.serializeMetadata(parser.flagConstraints));

				newBlob.setProperty("InitialValidityLimit", String.valueOf(parser.endTime));
				newBlob.setProperty("OriginalFileName", part.getSubmittedFileName());
				newBlob.setProperty("Content-Type", part.getContentType());
				newBlob.setProperty("UploadedFrom", request.getRemoteHost());
				newBlob.setProperty("File-Size", String.valueOf(payload.length));
				newBlob.setProperty("Valid-From", String.valueOf(newBlob.startTime));
				newBlob.setProperty("Valid-Until", String.valueOf(newBlob.getEndTime()));

				if (newBlob.getProperty("Created") == null)
					newBlob.setProperty("Created", String.valueOf(GUIDUtils.epochTime(targetUUID)));

				// set the Content-MD5 metadata field if missing at this point
				newBlob.getMD5();
			}
			catch (NoSuchAlgorithmException | SecurityException e) {
				response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
				return;
			}

			UDPReceiver.addToCacheContent(newBlob);

			setHeaders(newBlob, response);

			response.setHeader("Location", getURLPrefix(request) + "/" + parser.path + "/" + parser.startTime + "/" + targetUUID.toString());

			response.sendError(HttpServletResponse.SC_CREATED);

			final SQLtoUDP udpSender = SQLtoUDP.getInstance();

			if (udpSender != null)
				udpSender.newObject(newBlob);
		}
	}

	@Override
	protected void doPut(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		try (Timing t = new Timing(monitor, "PUT_ms")) {
			if (REDIRECT_TO_UPSTREAM && request.getParameter("Force-Update") == null) {
				response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
				response.setHeader("Location", UPSTREAM_URL + request.getPathInfo());

				return;
			}

			response.sendError(HttpServletResponse.SC_NOT_IMPLEMENTED, "You cannot modify objects in the cache");
		}
	}

	@Override
	protected void doDelete(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		try (Timing t = new Timing(monitor, "DELETE_ms")) {
			if (REDIRECT_TO_UPSTREAM && request.getParameter("Force-Delete") == null) {
				response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
				response.setHeader("Location", UPSTREAM_URL + request.getPathInfo());

				return;
			}

			final RequestParser parser = new RequestParser(request);

			if (!parser.ok) {
				printUsage(request, response);
				return;
			}

			final Blob matchingObject = getMatchingObject(parser);

			if (matchingObject == null) {
				response.sendError(HttpServletResponse.SC_NOT_FOUND, "No matching objects found");
				return;
			}

			final List<SoftReference<Blob>> keyContent = UDPReceiver.currentCacheContent.get(matchingObject.getKey());

			if (keyContent != null) {
				synchronized (keyContent) {
					final Iterator<SoftReference<Blob>> it = keyContent.iterator();

					while (it.hasNext()) {
						final SoftReference<Blob> sb = it.next();

						final Blob b = sb.get();

						if (b == null)
							it.remove();
						else
							if (b.equals(matchingObject)) {
								it.remove();
								break;
							}
					}
				}
			}

			response.sendError(HttpServletResponse.SC_NO_CONTENT);
		}
	}

	private static Blob getMatchingObject(final RequestParser parser) {
		final List<SoftReference<Blob>> candidates = UDPReceiver.currentCacheContent.get(parser.path);

		if (candidates == null || candidates.size() == 0)
			return null;

		Blob bestMatch = null;

		for (final SoftReference<Blob> sblob : candidates) {
			final Blob blob = sblob.get();

			if (blob == null)
				continue;

			if (!blobMatchesParser(blob, parser))
				continue;

			// most recently created object wins
			if (bestMatch == null || blob.compareTo(bestMatch) < 0)
				bestMatch = blob;
		}

		try {
			if (bestMatch == null || !bestMatch.isComplete())
				return null;
		}
		catch (@SuppressWarnings("unused") NoSuchAlgorithmException | IOException e) {
			return null;
		}

		return bestMatch;
	}

	/**
	 * @param blob
	 * @param parser
	 * @return <code>true</code> if the request parameters are met by this in-memory object
	 */
	static boolean blobMatchesParser(final Blob blob, final RequestParser parser) {
		if (parser.uuidConstraint != null && !blob.getUuid().equals(parser.uuidConstraint))
			return false;

		if (parser.startTimeSet && !blob.covers(parser.startTime))
			return false;

		if (parser.notAfter > 0 && blob.getCreateTime() > parser.notAfter)
			return false;

		if (!blob.matches(parser.flagConstraints))
			return false;

		return true;
	}

	@Override
	protected void doOptions(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		response.setHeader("Allow", "GET, HEAD, POST, PUT, DELETE, OPTIONS");
		response.setHeader("Accept-Ranges", "bytes");

		final RequestParser parser = new RequestParser(request);

		if (!parser.ok)
			return;

		final Blob matchingObject = getMatchingObject(parser);

		if (matchingObject == null) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, "No matching objects found");
			return;
		}

		setHeaders(matchingObject, response);
	}
}
