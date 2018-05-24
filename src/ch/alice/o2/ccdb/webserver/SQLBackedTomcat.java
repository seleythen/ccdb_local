package ch.alice.o2.ccdb.webserver;

import javax.servlet.ServletException;

import org.apache.catalina.LifecycleException;

import ch.alice.o2.ccdb.servlets.SQLBacked;
import ch.alice.o2.ccdb.servlets.SQLBrowse;
import ch.alice.o2.ccdb.servlets.SQLDownload;

/**
 * Start an embedded Tomcat with the local servlet mapping (*:8080/Local/) by default
 *
 * @author costing
 * @since 2017-10-13
 */
public class SQLBackedTomcat {

	/**
	 * @param args
	 * @throws ServletException
	 */
	public static void main(final String[] args) throws ServletException {
		EmbeddedTomcat tomcat;

		try {
			tomcat = new EmbeddedTomcat("*");
		} catch (final ServletException se) {
			System.err.println("Cannot create the Tomcat server: " + se.getMessage());
			return;
		}

		tomcat.addServlet(SQLDownload.class.getName(), "/download/*");
		tomcat.addServlet(SQLBrowse.class.getName(), "/browse/*");
		tomcat.addServlet(SQLBrowse.class.getName(), "/latest/*");
		tomcat.addServlet(SQLBacked.class.getName(), "/*");

		// Start the server
		try {
			tomcat.start();
		} catch (final LifecycleException le) {
			System.err.println("Cannot start the Tomcat server: " + le.getMessage());
			return;
		}

		if (tomcat.debugLevel >= 1)
			System.err.println("Ready to accept HTTP calls on " + tomcat.address + ":" + tomcat.getPort());

		tomcat.blockWaiting();
	}
}
