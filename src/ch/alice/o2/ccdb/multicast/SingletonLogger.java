package ch.alice.o2.ccdb.multicast;

import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import alien.test.cassandra.tomcat.Options;

/**
 * @author ddosaru
 *
 */
public class SingletonLogger {
	private static Logger logger = Logger.getLogger(SingletonLogger.class.getCanonicalName());
	private static FileHandler fh;
	private static Formatter sf;

	private static void initLogger() {
		final String logFile = Options.getOption("multicast.logfile", null);

		if (logFile != null) {
			// Create the log file
			try {
				fh = new FileHandler(logFile, true);
			}
			catch (final Exception e) {
				e.printStackTrace();
			}

			sf = new SimpleFormatter();
			fh.setFormatter(sf);

			for (final Handler h : logger.getHandlers())
				logger.removeHandler(h);

			logger.addHandler(fh);
		}
		else {
			logger.setLevel(Level.OFF);
		}
	}

	static {
		initLogger();
	}

	/**
	 * @return the logger
	 */
	public static Logger getLogger() {
		return logger;
	}
}
