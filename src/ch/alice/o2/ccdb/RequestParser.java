package ch.alice.o2.ccdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

/**
 * Parse the request parameters and make the constraints available to the application
 *
 * @author costing
 * @since 2017-09-22
 */
class RequestParser {
	boolean ok = false;

	String path;

	long startTime = System.currentTimeMillis();
	long endTime = startTime + 1;
	UUID uuidConstraint = null;
	UUID cachedValue = null;

	boolean startTimeSet = false;
	boolean endTimeSet = false;

	long notAfter = 0;

	final Map<String, String> flagConstraints = new HashMap<>();

	public RequestParser(final HttpServletRequest request) {
		final String pathInfo = request.getPathInfo();

		if (pathInfo == null || pathInfo.isEmpty())
			return;

		final StringTokenizer st = new StringTokenizer(pathInfo, "/");

		if (st.countTokens() < 2)
			return;

		final List<String> pathElements = new ArrayList<>();

		ok = true;

		try {
			String previousUUID = request.getHeader("If-None-Match");

			if (previousUUID.indexOf('"') >= 0)
				previousUUID = previousUUID.substring(previousUUID.indexOf('"') + 1, previousUUID.lastIndexOf('"'));

			if (previousUUID != null && previousUUID.length() > 0)
				cachedValue = UUID.fromString(previousUUID);
		} catch (@SuppressWarnings("unused") final Throwable t) {
			// ignore
		}

		try {
			notAfter = Long.parseLong(request.getHeader("If-Not-After"));
		} catch (@SuppressWarnings("unused") final Throwable t) {
			// ignore
		}

		// search for path tokens, stop at the first numeric value which would be the start time
		while (st.hasMoreTokens()) {
			final String token = st.nextToken();

			if (token.isEmpty() || token.indexOf(0) >= 0)
				continue;

			try {
				final long tmp = Long.parseLong(token);

				startTime = tmp;
				endTime = startTime + 1;
				startTimeSet = true;
				break;
			} catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
				pathElements.add(token);
			}
		}

		// require at least two path elements, but not more than 10 (safety limit)
		if (pathElements.size() < 2 || pathElements.size() > 10 || !startTimeSet) {
			ok = false;
			return;
		}

		// optional arguments after the path and start time are end time, flags and UUID
		while (st.hasMoreTokens()) {
			final String token = st.nextToken();

			final int idx = token.indexOf('=');

			if (idx >= 0)
				flagConstraints.put(token.substring(0, idx).trim(), token.substring(idx + 1).trim());
			else
				try {
					final long tmp = Long.parseLong(token);

					if (!endTimeSet && endTime > startTime) {
						endTime = tmp;
						endTimeSet = true;
					}
					else {
						// unexpected time constraint showing up in the request
						ok = false;
						return;
					}
				} catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
					try {
						uuidConstraint = UUID.fromString(token);
					} catch (@SuppressWarnings("unused") final IllegalArgumentException iae) {
						ok = false;
						return;
					}
				}
		}

		final StringBuilder pathBuilder = new StringBuilder();

		for (final String token : pathElements) {
			if (pathBuilder.length() > 0)
				pathBuilder.append('/');

			pathBuilder.append(token);
		}

		path = pathBuilder.toString();
	}
}