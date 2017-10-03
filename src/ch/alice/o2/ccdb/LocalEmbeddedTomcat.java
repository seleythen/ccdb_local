package ch.alice.o2.ccdb;

import java.io.File;
import java.util.logging.LogManager;

import javax.servlet.ServletException;

import org.apache.catalina.Globals;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.Wrapper;
import org.apache.catalina.core.StandardContext;
import org.apache.catalina.startup.Tomcat;

/**
 * Start an embedded Tomcat with the local servlet mapping (localhost:8080/Local/) by default
 *
 * @author costing
 * @since 2017-09-26
 */
public class LocalEmbeddedTomcat {

	/**
	 * @param args
	 * @throws ServletException
	 * @throws LifecycleException
	 */
	public static void main(final String[] args) throws ServletException, LifecycleException {
		// This is to disable Tomcat from creating work directories, nothing needs to be compiled on the fly
		System.setProperty(Globals.CATALINA_HOME_PROP, System.getProperty("java.io.tmpdir"));

		final int debugLevel = Options.getIntOption("tomcat.debug", 1);

		// Disable all console logging by default
		if (debugLevel < 2)
			LogManager.getLogManager().reset();

		// Start an embedded Tomcat instance listening on the indicated port and address
		final Tomcat tomcat = new Tomcat();

		final int port = Options.getIntOption("tomcat.port", 8080);
		final String address = Options.getOption("tomcat.address", "localhost");

		tomcat.setPort(port);
		tomcat.getConnector().setProperty("address", address);

		// Add a dummy ROOT context
		final StandardContext ctx = (StandardContext) tomcat.addWebapp("", new File(System.getProperty("java.io.tmpdir")).getAbsolutePath());

		// disable per context work directories too
		ctx.setWorkDir(".");

		// This is our one and only servlet to run
		final Wrapper wrapper = Tomcat.addServlet(ctx, "Local", Local.class.getName());
		wrapper.addMapping("/*");
		wrapper.setLoadOnStartup(0);

		// Start the server
		tomcat.start();

		if (tomcat.getService().findConnectors()[0].getState() == LifecycleState.FAILED) {
			System.err.println("Failed to start the embedded Tomcat listening on " + address + ":" + port + ".");

			if (debugLevel < 2)
				System.err.println("Set -Dtomcat.debug=2 (or export TOMCAT_DEBUG=2) to see the logging messages from the server.");

			System.exit(1);
		}

		if (debugLevel >= 1)
			System.err.println("Ready to accept HTTP calls on " + address + ":" + port + ", file repository base path is: " + Local.basePath);

		tomcat.getServer().await();
	}
}
