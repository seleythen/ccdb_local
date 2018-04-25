package ch.alice.o2.ccdb.servlets;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

import ch.alice.o2.ccdb.Options;
import ch.alice.o2.ccdb.RequestParser;

/**
 * SQL-backed implementation of CCDB. File reside on a separate storage and clients are redirected to it for the actual file access
 *
 * @author costing
 * @since 2017-10-13
 */
@WebServlet("/SQLBacked/*")
@MultipartConfig
public class SQLBacked extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * The base path of the file repository
	 */
	public static final String basePath = Options.getOption("file.repository.location", System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "QC");

	private static String getURLPrefix(final HttpServletRequest request) {
		return request.getContextPath() + request.getServletPath();
	}

	@Override
	protected void doHead(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response, true);
	}

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response, false);
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

		final SQLObject matchingObject = SQLObject.getMatchingObject(parser);

		if (matchingObject == null) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, "No matching objects found");
			return;
		}

		if (parser.cachedValue != null && matchingObject.id.equals(parser.cachedValue)) {
			response.sendError(HttpServletResponse.SC_NOT_MODIFIED);
			return;
		}

		// Can I serve it directly?

		if (matchingObject.replicas.contains(Integer.valueOf(0))) {
			SQLDownload.download(head, matchingObject, request, response);
			return;
		}

		setHeaders(matchingObject, response);

		response.sendRedirect(getURLPrefix(request) + matchingObject.getAddresses().iterator().next());
	}

	static void setMD5Header(final SQLObject obj, final HttpServletResponse response) {
		if (obj.md5 != null && !obj.md5.isEmpty())
			response.setHeader("Content-MD5", obj.md5);
	}

	static void setHeaders(final SQLObject obj, final HttpServletResponse response) {
		response.setDateHeader("Date", System.currentTimeMillis());
		response.setHeader("Valid-Until", String.valueOf(obj.validUntil));
		response.setHeader("Valid-From", String.valueOf(obj.validFrom));

		if (obj.initialValidity != obj.validUntil)
			response.setHeader("InitialValidityLimit", String.valueOf(obj.initialValidity));

		response.setHeader("Created", String.valueOf(obj.createTime));
		response.setHeader("ETag", "\"" + obj.id + "\"");

		response.setDateHeader("Last-Modified", obj.getLastModified());

		for (final Map.Entry<Integer, String> metadataEntry : obj.metadata.entrySet()) {
			final String mdKey = SQLObject.getMetadataString(metadataEntry.getKey());

			if (mdKey != null)
				response.setHeader(mdKey, metadataEntry.getValue());
		}
	}

	@Override
	protected void doPost(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
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

		final Part part = parts.iterator().next();

		final SQLObject newObject = new SQLObject(request, parser.path);

		final File targetFile = newObject.getLocalFile(true);

		if (targetFile == null) {
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Cannot create target file to perform the upload");
			return;
		}

		final MessageDigest md5;

		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (@SuppressWarnings("unused") NoSuchAlgorithmException e) {
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Cannot initialize the MD5 digester");
			return;
		}

		newObject.size = 0;

		try (FileOutputStream fos = new FileOutputStream(targetFile); InputStream is = part.getInputStream()) {
			final byte[] buffer = new byte[1024 * 16];

			int n;

			while ((n = is.read(buffer)) >= 0) {
				fos.write(buffer, 0, n);
				md5.update(buffer, 0, n);
				newObject.size += n;
			}
		} catch (@SuppressWarnings("unused") final IOException ioe) {
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Cannot upload the blob to the local file " + targetFile.getAbsolutePath());
			targetFile.delete();
			return;
		}

		for (final Map.Entry<String, String> constraint : parser.flagConstraints.entrySet())
			newObject.setProperty(constraint.getKey(), constraint.getValue());

		newObject.uploadedFrom = request.getRemoteHost();
		newObject.fileName = part.getSubmittedFileName();
		newObject.contentType = part.getContentType();
		newObject.md5 = String.format("%032x", new BigInteger(1, md5.digest())); // UUIDTools.getMD5(targetFile);

		newObject.replicas.add(Integer.valueOf(0));

		newObject.validFrom = parser.startTime;

		newObject.setValidityLimit(parser.endTime);

		newObject.save(request);

		// TODO queue for replication to EOS/AliEn

		setHeaders(newObject, response);

		response.setHeader("Location", newObject.getAddresses().iterator().next());
		response.sendError(HttpServletResponse.SC_CREATED);
	}

	@Override
	protected void doPut(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		final RequestParser parser = new RequestParser(request);

		if (!parser.ok) {
			printUsage(request, response);
			return;
		}

		final SQLObject matchingObject = SQLObject.getMatchingObject(parser);

		if (matchingObject == null) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, "No matching objects found");
			return;
		}

		for (final Map.Entry<String, String[]> param : request.getParameterMap().entrySet())
			if (param.getValue().length > 0)
				matchingObject.setProperty(param.getKey(), param.getValue()[0]);

		if (parser.endTimeSet)
			matchingObject.setValidityLimit(parser.endTime);

		final boolean changed = matchingObject.save(request);

		setHeaders(matchingObject, response);

		response.setHeader("Location", matchingObject.getAddresses().iterator().next());

		if (changed)
			response.sendError(HttpServletResponse.SC_NO_CONTENT);
		else
			response.sendError(HttpServletResponse.SC_NOT_MODIFIED);
	}

	@Override
	protected void doDelete(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		final RequestParser parser = new RequestParser(request);

		if (!parser.ok) {
			printUsage(request, response);
			return;
		}

		final SQLObject matchingObject = SQLObject.getMatchingObject(parser);

		if (matchingObject == null) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, "No matching objects found");
			return;
		}

		if (!matchingObject.delete()) {
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Could not delete the underlying record");
			return;
		}

		for (final Integer replica : matchingObject.replicas)
			if (replica.intValue() == 0) {
				// local file
				final File f = matchingObject.getLocalFile(false);

				if (f != null)
					f.delete();
			}

		response.sendError(HttpServletResponse.SC_NO_CONTENT);
	}

	private static void printUsage(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
		response.setContentType("text/plain");

		try (PrintWriter pw = response.getWriter()) {
			pw.append("Usage of /*:\n\n");
			pw.append("GET:\n  task name / detector name / start time / <UUID> - return the content of that UUID, or\n  task name / detector name / [ / time [ / key = value]* ]\n\n");
			pw.append("POST:\n  task name / detector name / start time [ / end time ] [ / UUID ] [ / key = value ]*\n  binary blob as multipart parameter called 'blob'\n\n");
			pw.append("PUT:\n  task name / detector name / start time [ / new end time ] [ / UUID ] [? (key=newvalue&)* ]\n\n");
			pw.append("DELETE:\n  task name / detector name / start time / UUID\n  or any other selection string, the matching object will be deleted\n\n");
			pw.append("Was called with:\n  servlet path: " + request.getServletPath());
			pw.append("\n  context path: " + request.getContextPath());
			pw.append("\n  HTTP method: " + request.getMethod());
			pw.append("\n  path info: " + request.getPathInfo());
			pw.append("\n  query string: " + request.getQueryString());
			pw.append("\n  request URI: " + request.getRequestURI());
			pw.append("\n");
		}
	}

	@Override
	protected void doOptions(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		response.setHeader("Allow", "GET, POST, PUT, DELETE, OPTIONS");

		final RequestParser parser = new RequestParser(request);

		if (!parser.ok)
			return;

		final SQLObject matchingObject = SQLObject.getMatchingObject(parser);

		if (matchingObject == null) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, "No matching objects found");
			return;
		}

		setHeaders(matchingObject, response);
	}
}
