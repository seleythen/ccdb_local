/**
 *
 */
package ch.alice.o2.ccdb.servlets;

import javax.servlet.http.HttpServletResponse;

/**
 * @author costing
 * @since Oct 30, 2020
 */
public class CCDBUtils {

	/**
	 * Common way to send the client to a different location. It will use HTTP 303 instead of the default 302, asking the client to do a GET request to the alternative location
	 * It will however not encourage the client to remember this location for subsequent requests.
	 *
	 * @param response
	 * @param url
	 */
	public static void sendRedirect(final HttpServletResponse response, final String url) {
		response.setStatus(HttpServletResponse.SC_SEE_OTHER);
		response.setHeader("Location", url);
	}

	/**
	 * Set headers to disable caching of the given response
	 *
	 * @param response
	 */
	public static void disableCaching(final HttpServletResponse response) {
		response.setHeader("Cache-Control", "no-cache");
	}
}
