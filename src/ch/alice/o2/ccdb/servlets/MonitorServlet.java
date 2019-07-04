package ch.alice.o2.ccdb.servlets;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.MonitoringDataCache;
import alien.monitoring.Timing;
import ch.alice.o2.ccdb.RequestParser;
import lazyj.Format;

/**
 * Dump most recent monitoring data in JSON format
 *
 * @author costing
 * @since 2019-07-03
 */
@WebServlet("/monitor/*")
public class MonitorServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	private static final Monitor monitor = MonitorFactory.getMonitor(MonitorServlet.class.getCanonicalName());

	private static final MonitoringDataCache lastValuesCache = new MonitoringDataCache();

	static {
		MonitorFactory.getMonitorDataSender().add(lastValuesCache);
	}

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		try (Timing timing = new Timing(monitor, "GET_ms")) {
			final RequestParser parser = new RequestParser(request, true);

			Pattern pCluster = null;
			Pattern pNode = null;
			Pattern pParam = null;

			if (parser.path != null && parser.path.length() > 0) {
				final StringTokenizer st = new StringTokenizer(parser.path, "/");

				if (st.hasMoreTokens())
					pCluster = patternFrom(st.nextToken());

				if (st.hasMoreTokens())
					pNode = patternFrom(st.nextToken());

				if (st.hasMoreTokens())
					pParam = patternFrom(st.nextToken());
			}

			response.setContentType("application/json");
			// response.setContentType("text/plain");

			final Map<String, Map<String, Map<String, Map.Entry<Long, Object>>>> tree = new TreeMap<>();

			for (final String key : lastValuesCache.getKeys()) {
				final Map.Entry<Long, Object> value = lastValuesCache.get(key);

				if (value == null)
					continue;

				final StringTokenizer st = new StringTokenizer(key, "/");

				final String cluster = st.nextToken();

				if (pCluster != null && !pCluster.matcher(cluster).matches())
					continue;

				final String node = st.nextToken();

				if (pNode != null && !pNode.matcher(node).matches())
					continue;

				final String param = st.nextToken();

				if (pParam != null && !pParam.matcher(param).matches())
					continue;

				final Map<String, Map<String, Map.Entry<Long, Object>>> nodeTree = tree.computeIfAbsent(cluster, k -> new TreeMap<>());
				final Map<String, Map.Entry<Long, Object>> params = nodeTree.computeIfAbsent(node, k -> new TreeMap<>());

				params.put(param, value);
			}

			try (PrintWriter pw = new PrintWriter(response.getOutputStream())) {
				boolean l1 = false;

				pw.println("{");

				for (final Map.Entry<String, Map<String, Map<String, Map.Entry<Long, Object>>>> topLevelEntry : tree.entrySet()) {
					if (l1)
						pw.print(",");
					else
						l1 = true;

					pw.print("  \"");
					pw.print(Format.escJSON(topLevelEntry.getKey()));
					pw.println("\": {");

					boolean l2 = false;

					for (final Map.Entry<String, Map<String, Map.Entry<Long, Object>>> secondLevelEntry : topLevelEntry.getValue().entrySet()) {
						if (l2)
							pw.print(",");
						else
							l2 = true;

						pw.print("    \"");
						pw.print(Format.escJSON(secondLevelEntry.getKey()));
						pw.println("\": [");

						boolean l3 = false;

						for (final Map.Entry<String, Map.Entry<Long, Object>> thirdLevelEntry : secondLevelEntry.getValue().entrySet()) {
							if (l3)
								pw.print(",");
							else
								l3 = true;

							pw.print("      {\"param\": \"");
							pw.print(Format.escJSON(thirdLevelEntry.getKey()));
							pw.print("\", ");
							pw.print("\"updated\": ");
							pw.print(thirdLevelEntry.getValue().getKey());
							pw.print(", \"value\": ");

							final Object toPrint = thirdLevelEntry.getValue().getValue();

							if (toPrint instanceof Number)
								pw.print(toPrint.toString());
							else {
								pw.print("\"");
								pw.print(Format.escJSON(toPrint.toString()));
								pw.print("\"");
							}
							pw.println("}");
						}

						pw.println("    ]");
					}

					pw.println("  }");
				}

				pw.println("}");
			}
		}

	}

	private static Pattern patternFrom(final String param) {
		if (param.length() > 0)
			return Pattern.compile("^" + param + "$");

		return null;
	}
}
