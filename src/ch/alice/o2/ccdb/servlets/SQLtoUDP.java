package ch.alice.o2.ccdb.servlets;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import ch.alice.o2.ccdb.Options;
import ch.alice.o2.ccdb.multicast.Blob;

/**
 * Send newly uploaded objects to one or more UDP targets (multicast or unicast)
 *
 * @author costing
 * @since 2019-10-31
 */
public class SQLtoUDP implements SQLNotifier {
	private static class HostAndPort {
		final String host;
		final int port;

		public HostAndPort(final String host, final int port) {
			this.host = host;
			this.port = port;
		}

		@Override
		public String toString() {
			return this.host + ":" + this.port;
		}
	}

	private List<HostAndPort> destinations = new LinkedList<>();

	private SQLtoUDP() {
		String udpNotifications = Options.getOption("udp.targets", null);

		if (udpNotifications != null) {
			final StringTokenizer st = new StringTokenizer(udpNotifications, " \r\t\n\f,;");

			try {
				while (st.hasMoreTokens()) {
					String host = st.nextToken();

					final int column = host.lastIndexOf(':');
					final int bracket = host.lastIndexOf(']');

					int port = 3342;

					if (column > 0 && (bracket < 0 || column > bracket)) {
						port = Integer.parseInt(host.substring(column + 1));
						host = host.substring(0, column);
					}

					destinations.add(new HostAndPort(host, port));
				}
			}
			catch (final Throwable t) {
				System.err.println("Exception parsing host:port list `" + udpNotifications + "` : " + t.getMessage());
			}
		}
	}

	private static SQLtoUDP instance = null;

	static synchronized SQLtoUDP getInstance() {
		if (instance == null) {
			final SQLtoUDP attempt = new SQLtoUDP();

			if (attempt.destinations.size() > 0) {
				instance = attempt;

				System.err.println("Will send new objects to " + instance.destinations);
			}
		}

		return instance;
	}

	@Override
	public String toString() {
		return "SQLtoUDP @ " + destinations;
	}

	@Override
	public void newObject(final SQLObject object) {
		// notify all UDP receivers of the new object
		try {
			final Blob b = new Blob(object);

			for (HostAndPort destination : destinations)
				b.send(destination.host, destination.port);
		}
		catch (NoSuchAlgorithmException | IOException e) {
			System.err.println("Exception sending Blob on UDP: " + e.getMessage());
		}
	}

	@Override
	public void deletedObject(final SQLObject object) {
		// nothing to do
	}

	@Override
	public void updatedObject(final SQLObject object) {
		// nothing to do on update, the underlying backend doesn't need to know of metadata changes
	}
}
