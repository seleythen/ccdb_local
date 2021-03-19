/**
 * Display an initially empty frame, load JSRoot in it and pass the file name and content URL to JSRoot to be displayed interactively.
 * As this class is independent of the actual backend, all three servers instantiate it at the same location: <code>/JSRoot</code>
 */
package ch.alice.o2.ccdb.servlets;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import lazyj.Format;

/**
 * @author costing
 * @since Jan 21, 2021
 */
public class JSRoot extends HttpServlet {
	private static final long serialVersionUID = 1L;

	private static final Monitor monitor = MonitorFactory.getMonitor(JSRoot.class.getCanonicalName());

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		final String path = request.getParameter("f");
		final String fileName = request.getParameter("n");

		if (path == null || fileName == null || path.length() < 0 || fileName.length() < 0) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid parameters");
			return;
		}

		response.setContentType("text/html");

		try (Timing t = new Timing(monitor, "GET_ms"); ServletOutputStream out = response.getOutputStream()) {
			out.println("<!DOCTYPE html>\n"
					+ "<html lang=\"en\">\n"
					+ "   <head>\n"
					+ "      <meta charset=\"UTF-8\" http-equiv=\"X-UA-Compatible\" content=\"IE=Edge\">\n"
					+ "      <title>"+Format.escHtml(fileName)+"- ROOT JS viewer</title>\n"
					+ "      <script type=\"text/javascript\" src=\"https://root.cern.ch/js/latest/scripts/JSRootCore.js?gui\"></script>\n"
					+ "   </head>\n"
					+ "   <body>\n"
					+ "      <div id=\"simpleGUI\" noselect=\"file\" topname=\""+Format.escHtml(fileName)+"\" file=\""+Format.escHtml(path)+"\">\n"
					+ "         loading scripts ...\n"
					+ "      </div>\n"
					+ "   </body>\n"
					+ "</html>\n"
					);
		}
	}
}
