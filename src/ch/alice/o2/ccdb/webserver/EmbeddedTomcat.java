package ch.alice.o2.ccdb.webserver;

import java.io.File;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;

import org.apache.catalina.Globals;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.Service;
import org.apache.catalina.Valve;
import org.apache.catalina.Wrapper;
import org.apache.catalina.authenticator.SSLAuthenticator;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.core.StandardContext;
import org.apache.catalina.core.StandardHost;
import org.apache.catalina.startup.Tomcat;
import org.apache.catalina.valves.ErrorReportValve;
import org.apache.tomcat.util.descriptor.web.LoginConfig;
import org.apache.tomcat.util.descriptor.web.SecurityCollection;
import org.apache.tomcat.util.descriptor.web.SecurityConstraint;

import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.user.JAKeyStore;
import alien.user.LdapCertificateRealm;
import alien.user.UserFactory;
import ch.alice.o2.ccdb.Options;

/**
 * Configure an embedded Tomcat instance
 *
 * @author costing
 * @since 2017-10-13
 */
public class EmbeddedTomcat extends Tomcat {
	static transient final Monitor monitor = MonitorFactory.getMonitor(EmbeddedTomcat.class.getCanonicalName());

	static {
		System.setProperty(Globals.CATALINA_HOME_PROP, System.getProperty("java.io.tmpdir"));
	}

	final int debugLevel;
	final String address;
	final StandardContext ctx;

	/**
	 * @param defaultAddress
	 *            default listening address for the Tomcat server. Either "localhost" (default for testing servers) or "*" for production instances.
	 * @throws ServletException
	 */
	public EmbeddedTomcat(final String defaultAddress) throws ServletException {
		super();

		// This is to disable Tomcat from creating work directories, nothing needs to be compiled on the fly
		debugLevel = Options.getIntOption("tomcat.debug", 1);

		// Disable all console logging by default
		if (debugLevel < 2)
			LogManager.getLogManager().reset();

		if (debugLevel > 2) {
			Logger.getLogger("org.apache.catalina").setLevel(Level.FINEST);
			Logger.getLogger("alien").setLevel(Level.FINEST);
		}

		address = Options.getOption("tomcat.address", defaultAddress);

		setPort(Options.getIntOption("tomcat.port", 8080));

		decorateConnector(getConnector());

		// Add a dummy ROOT context
		ctx = (StandardContext) addContext(getHost(), "", null);

		if (Options.getIntOption("ccdb.ssl", 0) > 0)
			initializeSSLEndpoint();
		else
			getConnector().setRedirectPort(0);
	}

	private void decorateConnector(final Connector connector) {
		connector.setProperty("address", address);
		connector.setProperty("maxKeepAliveRequests", String.valueOf(Options.getIntOption("maxKeepAliveRequests", 1000)));

		// large headers are needed since alternate locations include access envelopes, that are rather large (default is 8KB)
		connector.setProperty("maxHttpHeaderSize", "100000");

		// same, let's allow for a lot of custom headers to be set (default is 100)
		connector.setProperty("maxHeaderCount", "1000");

		connector.setProperty("connectionTimeout", String.valueOf(Options.getIntOption("connectionTimeout", 10000))); // clients should be quick

		connector.setProperty("disableUploadTimeout", "false");
		connector.setProperty("connectionUploadTimeout", String.valueOf(Options.getIntOption("connectionTimeout", 300000))); // 5 minutes max to upload an object
	}

	/**
	 * @param connector
	 * @return <code>true</code> if monitoring was attached to this connector
	 */
	public static boolean attachMonitoring(final Connector connector) {
		final Executor executor = connector.getProtocolHandler().getExecutor();

		if (executor instanceof ThreadPoolExecutor) {
			final ThreadPoolExecutor tpe = (ThreadPoolExecutor) executor;

			monitor.addMonitoring("server_status_" + connector.getPort(), (names, values) -> {
				names.add("active_threads");
				values.add(Double.valueOf(tpe.getActiveCount()));

				names.add("max_threads");
				values.add(Double.valueOf(tpe.getMaximumPoolSize()));
			});

			return true;
		}

		System.err.println("Cannot monitor Tomcat executor on port " + connector.getPort() + (executor != null ? " of type " + executor.getClass().getCanonicalName() : ""));
		return false;
	}

	/**
	 * @param className
	 * @param mapping
	 * @return the newly created wrapper around the
	 */
	public Wrapper addServlet(final String className, final String mapping) {
		final Wrapper wrapper = Tomcat.addServlet(ctx, className.substring(className.lastIndexOf('.') + 1), className);
		wrapper.addMapping(mapping);
		wrapper.setLoadOnStartup(0);
		return wrapper;
	}

	@Override
	public void start() throws LifecycleException {
		super.start();

		for (final Connector connector : getService().findConnectors())
			if (connector.getState() == LifecycleState.FAILED) {
				System.err.println("Failed to start the embedded Tomcat listening on " + address + ":" + connector.getPort() + ".");

				if (debugLevel < 2)
					System.err.println("Set -Dtomcat.debug=2 (or export TOMCAT_DEBUG=2) to see the logging messages from the server.");

				throw new LifecycleException("Cannot bind on " + address + ":" + port);
			}

		// everything is ok, we can start monitoring the pools
		for (final Connector connector : getService().findConnectors())
			attachMonitoring(connector);

		final StandardHost host = (StandardHost) getHost();

		for (final Valve v : host.getPipeline().getValves())
			if (v instanceof ErrorReportValve) {
				final ErrorReportValve erv = (ErrorReportValve) v;
				erv.setShowServerInfo(false);
			}
	}

	/**
	 * @return the port for the default connector
	 */
	public int getPort() {
		return port;
	}

	/**
	 * Block forever waiting for the server to exit (will never do normally)
	 */
	public void blockWaiting() {
		getServer().await();
	}

	/**
	 * @return <code>true</code> if the SSL endpoint could be created ok, <code>false</code> otherwise
	 */
	public boolean initializeSSLEndpoint() {
		final Service service = getService();
		try {
			final Connector sslConnector = createSslConnector();
			service.addConnector(sslConnector);
		}
		catch (final Exception e) {
			System.err.println("Could not initialize the SSL connector: " + e.getMessage());
			return false;
		}

		final LdapCertificateRealm ldapRealm = new LdapCertificateRealm();
		ldapRealm.setTransportGuaranteeRedirectStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
		getEngine().setRealm(ldapRealm);

		// Use AliEn's LDAP to look up users and roles
		final LoginConfig loginConfig = new LoginConfig();
		loginConfig.setRealmName(LdapCertificateRealm.class.getCanonicalName());
		loginConfig.setAuthMethod("CLIENT-CERT");

		ctx.setLoginConfig(loginConfig);

		if (Options.getIntOption("ccdb.ssl.enforce", 1) > 1) {
			addSecurityConstraint(getAddOrUpdateConstraint());
			addSecurityConstraint(getRemovalConstraint());
		}

		ctx.setRealm(ldapRealm);
		ctx.getPipeline().addValve(new SSLAuthenticator());

		return true;
	}

	private void addSecurityConstraint(final SecurityConstraint constraint) {
		for (final String role : constraint.findAuthRoles())
			if (!ctx.findSecurityRole(role))
				ctx.addSecurityRole(role);

		ctx.addConstraint(constraint);
	}

	private static SecurityConstraint getAddOrUpdateConstraint() {
		// Protect all write operations
		final SecurityCollection defaultPath = new SecurityCollection("defaultPath", "Default path protection");
		defaultPath.addPattern("/*");
		defaultPath.addMethod("POST");
		defaultPath.addMethod("PUT");

		// Require SSL for the data changing methods of the default path
		final SecurityConstraint addOrUpdateConstraint = new SecurityConstraint();
		addOrUpdateConstraint.setDisplayName("SSL certificate required");
		addOrUpdateConstraint.addCollection(defaultPath);
		addOrUpdateConstraint.addAuthRole(Options.getOption("ldap.role", "ccdb"));
		addOrUpdateConstraint.setAuthConstraint(true);
		addOrUpdateConstraint.setUserConstraint("CONFIDENTIAL");

		return addOrUpdateConstraint;
	}

	private static SecurityConstraint getRemovalConstraint() {
		final SecurityCollection truncateCalls = new SecurityCollection("truncateCalls", "Protect TRUNCATE calls");
		truncateCalls.addPattern("/truncate/*");

		final SecurityCollection defaultPathRemoval = new SecurityCollection("defaultPathRemoval", "Removal requests on the default path");
		defaultPathRemoval.addPattern("/*");
		defaultPathRemoval.addMethod("DELETE");

		// restrict access to /truncate/ and data removal requests to admin only
		final SecurityConstraint removalConstraint = new SecurityConstraint();
		removalConstraint.setDisplayName("SSL certificate required");
		removalConstraint.addCollection(truncateCalls);
		removalConstraint.addCollection(defaultPathRemoval);
		removalConstraint.addAuthRole("admin");
		removalConstraint.setAuthConstraint(true);
		removalConstraint.setUserConstraint("CONFIDENTIAL");

		return removalConstraint;
	}

	/**
	 * Create SSL connector for the Tomcat server
	 *
	 * @param tomcatPort
	 * @throws Exception
	 */
	private Connector createSslConnector() throws Exception {
		final int tomcatPort = Options.getIntOption("tomcat.port.ssl", 8443);

		getConnector().setRedirectPort(tomcatPort);

		final String keystorePass = new String(JAKeyStore.pass);

		final String dirName = System.getProperty("java.io.tmpdir") + File.separator;
		final String keystoreName = dirName + "keystore.jks_" + UserFactory.getUserID();
		final String truststoreName = dirName + "truststore.jks_" + UserFactory.getUserID();

		if (!JAKeyStore.saveKeyStore(JAKeyStore.getKeyStore(), keystoreName, JAKeyStore.pass))
			return null;

		if (!JAKeyStore.saveKeyStore(JAKeyStore.trustStore, truststoreName, JAKeyStore.pass))
			return null;

		final Connector connector = new Connector("org.apache.coyote.http11.Http11NioProtocol");

		connector.setPort(tomcatPort);
		connector.setSecure(true);
		connector.setScheme("https");
		connector.setAttribute("keyAlias", "User.cert");
		connector.setAttribute("keystorePass", keystorePass);
		connector.setAttribute("keystoreType", "JKS");
		connector.setAttribute("keystoreFile", keystoreName);
		connector.setAttribute("truststorePass", keystorePass);
		connector.setAttribute("truststoreType", "JKS");
		connector.setAttribute("truststoreFile", truststoreName);
		connector.setAttribute("clientAuth", "true");
		connector.setAttribute("sslProtocol", "TLS");
		connector.setAttribute("SSLEnabled", "true");
		connector.setAttribute("maxThreads", "200");

		decorateConnector(connector);

		return connector;
	}
}
