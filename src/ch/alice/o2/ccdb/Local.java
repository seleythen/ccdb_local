package ch.alice.o2.ccdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

import org.apache.tomcat.util.http.fileupload.IOUtils;

/**
 * Prototype implementation of QC repository. This simple implementation is filesystem-based and targeted to local development and testing of the QC framework
 *
 * @author costing
 * @since 2017-09-20
 */
@WebServlet("/Local/*")
@MultipartConfig
public class Local extends HttpServlet {
	private static final long serialVersionUID = 1L;

	static final String basePath = Options.getOption("file.repository.location", System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "QC");

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

		final LocalObjectWithVersion matchingObject = getMatchingObject(parser);

		if (matchingObject == null) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, "No matching objects found");
			return;
		}

		if (parser.cachedValue != null && matchingObject.referenceFile.getName().equalsIgnoreCase(parser.cachedValue.toString())) {
			response.sendError(HttpServletResponse.SC_NOT_MODIFIED);
			return;
		}

		if (parser.uuidConstraint != null) {
			// download this explicitly requested file, unless HEAD was indicated in which case no content should be returned to the client

			setHeaders(matchingObject, response, true);

			if (!head)
				download(matchingObject, response);

			return;
		}

		setHeaders(matchingObject, response, false);

		response.sendRedirect(getURLPrefix(request) + matchingObject.referenceFile.getPath().substring(basePath.length()));
	}

	private static void setHeaders(final LocalObjectWithVersion obj, final HttpServletResponse response, final boolean forDownload) {
		response.setDateHeader("Date", System.currentTimeMillis());
		response.setHeader("Valid-Until", String.valueOf(obj.endTime));
		response.setHeader("Valid-From", String.valueOf(obj.startTime));
		response.setHeader("Created", String.valueOf(obj.getCreateTime()));
		response.setHeader("ETag", '"' + obj.referenceFile.getName() + '"');

		if (forDownload) {
			response.addHeader("Content-Disposition", "inline;filename=\"" + obj.getOriginalName() + "\"");
			response.setHeader("Content-Length", String.valueOf(obj.referenceFile.length()));
			response.setHeader("Content-Type", obj.getProperty("Content-Type", "application/octet-stream"));
		}

		try {
			response.setDateHeader("Last-Modified", Long.parseLong(obj.getProperty("Last-Modified")));
		} catch (@SuppressWarnings("unused") NullPointerException | NumberFormatException ignore) {
			response.setDateHeader("Last-Modified", (obj.getCreateTime()));
		}
	}

	private static void download(final LocalObjectWithVersion obj, final HttpServletResponse response) throws IOException {
		try (InputStream is = new FileInputStream(obj.referenceFile); OutputStream os = response.getOutputStream()) {
			IOUtils.copy(is, os);
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

		final File folder = new File(basePath + "/" + parser.path + "/" + parser.startTime);

		if (!folder.exists())
			if (!folder.mkdirs()) {
				response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Cannot create path " + folder.getAbsolutePath());
				return;
			}

		final UUID targetUUID = UUID.randomUUID();

		final Part part = parts.iterator().next();

		final File targetFile = new File(folder, targetUUID.toString());

		try (FileOutputStream fos = new FileOutputStream(targetFile)) {
			IOUtils.copy(part.getInputStream(), fos);
		}

		final LocalObjectWithVersion newObject = new LocalObjectWithVersion(parser.startTime, targetFile);

		for (final Map.Entry<String, String> constraint : parser.flagConstraints.entrySet())
			newObject.setProperty(constraint.getKey(), constraint.getValue());

		newObject.setProperty("InitialValidityLimit", String.valueOf(parser.endTime));
		newObject.setProperty("OriginalFileName", part.getSubmittedFileName());
		newObject.setProperty("Content-Type", part.getContentType());
		newObject.setProperty("UploadedFrom", request.getRemoteHost());

		if (newObject.getProperty("CreateTime") == null)
			newObject.setProperty("CreateTime", String.valueOf(System.currentTimeMillis()));

		newObject.setValidityLimit(parser.endTime);

		newObject.saveProperties(request.getRemoteHost());

		setHeaders(newObject, response, false);
		response.setHeader("Location", getURLPrefix(request) + "/" + parser.path + "/" + parser.startTime + "/" + targetUUID.toString());
		response.sendError(HttpServletResponse.SC_CREATED);
	}

	@Override
	protected void doPut(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		final RequestParser parser = new RequestParser(request);

		if (!parser.ok) {
			printUsage(request, response);
			return;
		}

		final LocalObjectWithVersion matchingObject = getMatchingObject(parser);

		if (matchingObject == null) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, "No matching objects found");
			return;
		}

		for (final Map.Entry<String, String[]> param : request.getParameterMap().entrySet())
			if (param.getValue().length > 0)
				matchingObject.setProperty(param.getKey(), param.getValue()[0]);

		if (parser.endTimeSet)
			matchingObject.setValidityLimit(parser.endTime);

		matchingObject.saveProperties(request.getRemoteHost());

		setHeaders(matchingObject, response, false);

		response.setHeader("Location", getURLPrefix(request) + matchingObject.referenceFile.getPath().substring(basePath.length()));

		if (matchingObject.taintedProperties)
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

		final LocalObjectWithVersion matchingObject = getMatchingObject(parser);

		if (matchingObject == null) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, "No matching objects found");
			return;
		}

		if (!matchingObject.referenceFile.delete()) {
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Could not delete the underlying file");
			return;
		}

		final File fProperties = new File(matchingObject.referenceFile.getPath() + ".properties");
		fProperties.delete();

		response.sendError(HttpServletResponse.SC_NO_CONTENT);
	}

	private static void printUsage(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
		response.setContentType("text/plain");

		try (PrintWriter pw = response.getWriter()) {
			pw.append("Usage:\n\n");
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
		}
	}

	private static LocalObjectWithVersion getMatchingObject(final RequestParser parser) {
		final File fBaseDir = new File(Local.basePath + "/" + parser.path);

		if (parser.startTime > 0 && parser.uuidConstraint != null) {
			// is this the full path to a file? if so then download it

			final File toDownload = new File(fBaseDir, parser.startTime + "/" + parser.uuidConstraint.toString());

			if (toDownload.exists() && toDownload.isFile())
				return new LocalObjectWithVersion(parser.startTime, toDownload);

			// a particular object was requested but it doesn't exist
			return null;
		}

		LocalObjectWithVersion mostRecent = null;

		final File[] baseDirListing = fBaseDir.listFiles((f) -> f.isDirectory());

		if (baseDirListing == null)
			return null;

		for (final File fInterval : baseDirListing)
			try {
				final long lValidityStart = Long.parseLong(fInterval.getName());

				final File[] intervalFileList = fInterval.listFiles((f) -> f.isFile() && !f.getName().contains("."));

				if (intervalFileList == null)
					continue;

				for (final File f : intervalFileList) {
					final LocalObjectWithVersion owv = new LocalObjectWithVersion(lValidityStart, f);

					if (owv.covers(parser.startTime) && owv.matches(parser.flagConstraints))
						if (mostRecent == null)
							mostRecent = owv;
						else
							if (owv.compareTo(mostRecent) < 0)
								mostRecent = owv;
				}
			} catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
				// ignore
			}

		return mostRecent;
	}
}
