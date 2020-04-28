/**
 * 
 */
package ch.alice.o2.ccdb.servlets.formatters;

import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import javax.servlet.http.HttpServletRequest;

/**
 * @author costing
 * @since Apr 27, 2020
 */
public class FormatterFactory {
	/**
	 * @param request
	 * @return the best formatter according to the requested content type by the client
	 */
	public static SQLFormatter getFormatter(final HttpServletRequest request) {
		SQLFormatter formatter = null;

		String sAccept = request.getParameter("Accept");

		if ((sAccept == null) || (sAccept.length() == 0))
			sAccept = request.getHeader("Accept");

		if ((sAccept == null || sAccept.length() == 0))
			sAccept = "text/plain";

		sAccept = sAccept.toLowerCase();

		Set<String> fieldFilter = null;

		String filterHeader = request.getHeader("X-Filter-Fields");

		if (filterHeader != null && !filterHeader.isBlank()) {
			final StringTokenizer st = new StringTokenizer(filterHeader, " \r\t\n,;");

			while (st.hasMoreTokens()) {
				String tok = st.nextToken().trim();

				if (tok.length() > 0) {
					if (fieldFilter == null)
						fieldFilter = new HashSet<>();

					fieldFilter.add(tok);
				}
			}
		}

		if ((sAccept.indexOf("application/json") >= 0) || (sAccept.indexOf("text/json") >= 0))
			formatter = new JSONFormatter(fieldFilter);
		else
			if (sAccept.indexOf("text/html") >= 0)
				formatter = new HTMLFormatter();
			else
				if (sAccept.indexOf("text/xml") >= 0)
					formatter = new XMLFormatter();
				else
					formatter = new TextFormatter();

		return formatter;
	}
}
