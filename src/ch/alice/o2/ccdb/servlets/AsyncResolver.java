/**
 * 
 */
package ch.alice.o2.ccdb.servlets;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import lazyj.Utils;
import lazyj.cache.ExpirationCache;
import utils.CachedThreadPool;

/**
 * @author costing
 * @since Oct 30, 2020
 */
public class AsyncResolver implements Runnable {
	private static ExpirationCache<String, String> closeSiteCache = new ExpirationCache<>(10000);

	private static ExecutorService asyncIPToSiteResolver = new CachedThreadPool(8, 1, TimeUnit.MINUTES);

	private final String ipAddress;

	private AsyncResolver(final String ipAddress) {
		this.ipAddress = ipAddress;
	}

	@Override
	public void run() {
		resolveIP(ipAddress);
	}

	private static String resolveIP(final String ipAddress) {
		try {
			final String site = Utils.download("http://alimonitor.cern.ch/services/getClosestSite.jsp?ip=" + ipAddress, null);

			if (site != null) {
				final String newValue = site.trim();

				closeSiteCache.put(ipAddress, newValue, 1000L * 60 * 30);

				return newValue;
			}
		}
		catch (@SuppressWarnings("unused") final IOException ioe) {
			// no reply yet
		}

		return null;
	}

	/**
	 * Get the client's closest site
	 *
	 * @param ip
	 *            IP address of the client
	 * @param asyncResolving
	 *            whether the matching should be done in-line or in background (only appying then to subsequent requests)
	 * @return the name of the closest site
	 */
	public static String getSite(final String ip, final boolean asyncResolving) {
		if (ip == null)
			return null;

		final String cacheValue = closeSiteCache.get(ip);

		if (cacheValue != null)
			return cacheValue;

		if (asyncResolving) {
			asyncIPToSiteResolver.submit(new AsyncResolver(ip));
			return null;
		}

		return resolveIP(ip);
	}
}
