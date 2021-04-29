/**
 *
 */
package ch.alice.o2.ccdb.tools;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import alien.test.cassandra.tomcat.Options;
import ch.alice.o2.ccdb.servlets.SQLObject;
import ch.alice.o2.ccdb.servlets.SQLtoHTTP;
import lazyj.Format;
import lazyj.Utils;

/**
 * @author costing
 * @since Nov 26, 2020
 */
public class Synchronization {
	private static Logger logger = Logger.getLogger(Synchronization.class.getCanonicalName());

	private static final AtomicInteger aiSynchronized = new AtomicInteger();
	private static final AtomicInteger aiSkipped = new AtomicInteger();
	private static final AtomicInteger aiError = new AtomicInteger();
	private static final AtomicInteger counter = new AtomicInteger();

	private static final AtomicLong alTotalSize = new AtomicLong();

	private static URL targetRepository;

	private static final HashSet<String> existingIDs = new HashSet<>();

	private static NodeList sourceObjects;

	private static String sourceRepo;
	private static String targetRepo;

	/**
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(final String[] args) throws InterruptedException {
		if (args.length != 3) {
			System.out.println("CCDB instances synchronization tool");
			System.out.println("Usage: ch.alice.o2.ccdb.Synchronization [source repository] [target repository] [path]\n");
			System.out.println("Example:");
			System.out.println("ch.alice.o2.ccdb.Synchronization http://ccdb-test.cern.ch:8080 http://localhost:8080 /qc/ITS");
			return;
		}

		sourceRepo = args[0].replaceAll("/+$", "");
		targetRepo = args[1].replaceAll("/+$", "");
		String path = args[2];

		if (!path.startsWith("/"))
			path = "/" + path;

		path = path.replaceAll("/+$", "");

		try {
			targetRepository = new URL(targetRepo);
		}
		catch (final MalformedURLException e1) {
			System.err.println("Invalid target URL(" + targetRepo + "): " + e1.getMessage());
			return;
		}

		System.out.println("Synchronizing " + sourceRepo + path + " to " + targetRepo + path);

		final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		// dbf.setValidating(true);

		DocumentBuilder dbTarget;
		try {
			dbTarget = dbf.newDocumentBuilder();
		}
		catch (final ParserConfigurationException e) {
			logger.log(Level.WARNING, "Cannot initialize the DocumentBuilder", e);
			return;
		}

		final Document docTarget;

		final String targetXmlPath = targetRepo + "/browse" + path + "*?Accept=text/xml";

		try {
			docTarget = dbTarget.parse(targetXmlPath);
		}
		catch (final IOException ioe) {
			System.err.println("Could not connect to " + targetRepo + " due to : " + ioe.getMessage());
			return;
		}
		catch (final SAXException e) {
			logger.log(Level.WARNING, "Cannot parse the reply from " + targetXmlPath, e);
			return;
		}

		final NodeList targetObjects = docTarget.getElementsByTagName("object");

		final int len = targetObjects.getLength();

		for (int i = 0; i < len; i++) {
			String id = targetObjects.item(i).getAttributes().getNamedItem("id").getNodeValue();

			if (id.startsWith("uuid"))
				id = id.substring(4);

			existingIDs.add(id);
		}

		System.out.println("Target found " + existingIDs.size() + " objects under " + path);

		DocumentBuilder dbSource;
		try {
			dbSource = dbf.newDocumentBuilder();
		}
		catch (final ParserConfigurationException e) {
			logger.log(Level.WARNING, "Cannot initialize the DocumentBuilder", e);
			return;
		}

		final String sourceXmlPath = sourceRepo + "/browse" + path + "*?Accept=text/xml";

		Document docSource;
		try {
			docSource = dbSource.parse(sourceXmlPath);
		}
		catch (final SAXException e) {
			System.err.println("Cannot parse the reply from " + targetXmlPath + " due to : " + e.getMessage());
			return;
		}
		catch (final IOException ioe) {
			System.err.println("Could not connect to " + sourceRepo + " due to : " + ioe.getMessage());
			return;
		}

		sourceObjects = docSource.getElementsByTagName("object");

		System.out.println("Source reports " + sourceObjects.getLength() + " objects under " + path);

		// docTarget.normalizeDocument();

		System.setProperty("http.targets", targetRepo);
		System.setProperty("file.repository.location", System.getProperty("user.dir") + "/temp");

		final int threads = Options.getIntOption("synchronization.threads", 8);

		System.out.println("Using SYNCHRONIZATION_THREADS=" + threads + " to perform the synchronization");

		final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();

		final ThreadPoolExecutor executor = new ThreadPoolExecutor(threads, threads, 1, TimeUnit.SECONDS, queue);

		final long lStart = System.currentTimeMillis();

		for (int i = 0; i < sourceObjects.getLength(); i++) {
			final Node n = sourceObjects.item(i);

			executor.submit(() -> checkEntity(n));
		}

		executor.shutdown();

		while (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
			System.err.println("Awaiting all threads to exit, queue length is " + queue.size() + ", active threads: " + executor.getActiveCount());
		}

		final long deltaSeconds = (System.currentTimeMillis() - lStart) / 1000;

		System.out.println("Skipped: " + aiSkipped + " files, failed to get: " + aiError + " files, synchronized: " + aiSynchronized + " files (" + Format.size(alTotalSize.get())
				+ (deltaSeconds > 0 ? ", " + Format.size(alTotalSize.get() / deltaSeconds) + "/s" : "") + ")");
	}

	/**
	 * @param i
	 * @param attributes
	 */
	private static void checkEntity(final Node n) {
		final NamedNodeMap attr = n.getAttributes();

		String id = attr.getNamedItem("id").getNodeValue();
		if (id.startsWith("uuid"))
			id = id.substring(4);

		final String sourcePath = attr.getNamedItem("path").getNodeValue();

		String message = "/" + sourceObjects.getLength() + ": " + sourcePath + "/" + id;

		try {
			if (!existingIDs.contains(id)) {
				message += " is missing";

				final String fileName = attr.getNamedItem("fileName").getNodeValue();

				final String validFrom = attr.getNamedItem("validFrom").getNodeValue();

				final String sourceURL = sourceRepo + "/" + sourcePath + "/" + validFrom + "/" + id + "?HTTPOnly=true";

				final SQLObject toUpload = new SQLObject(null, sourcePath, UUID.fromString(id));

				final File localFile = toUpload.getLocalFile(true);

				try {
					toUpload.size = Long.parseLong(attr.getNamedItem("size").getNodeValue());

					message += ", downloading (" + Format.size(toUpload.size) + ") ... ";

					final String downloadedIn = Utils.download(sourceURL, localFile.getAbsolutePath());

					if (downloadedIn == null) {
						System.err.println("Could not save to " + localFile.getAbsolutePath());
						aiError.incrementAndGet();
						return;
					}

					toUpload.fileName = fileName;
					toUpload.contentType = attr.getNamedItem("contentType").getNodeValue();
					toUpload.md5 = attr.getNamedItem("md5").getNodeValue();
					toUpload.lastModified = Long.parseLong(attr.getNamedItem("lastModified").getNodeValue());
					toUpload.validFrom = Long.parseLong(validFrom);
					toUpload.setValidityLimit(Long.parseLong(attr.getNamedItem("validUntil").getNodeValue()));

					final NodeList metadata = n.getChildNodes();

					for (int j = 0; j < metadata.getLength(); j++) {
						final Node m = metadata.item(j);

						if ("metadata".equals(m.getNodeName()))
							toUpload.setProperty(m.getAttributes().getNamedItem("key").getNodeValue(), m.getAttributes().getNamedItem("value").getNodeValue());
					}

					message += "uploading ... ";

					SQLtoHTTP.upload(toUpload, targetRepository);

					aiSynchronized.incrementAndGet();
					alTotalSize.addAndGet(toUpload.size);

					message += "done";
				}
				catch (@SuppressWarnings("unused") final IOException ioe) {
					message += "\nCould not download " + sourceURL + " to " + fileName;
					aiError.incrementAndGet();
				}
				finally {
					if (!localFile.delete())
						logger.log(Level.WARNING, "Cannot remove local file " + localFile.getAbsolutePath());
				}
			}
			else {
				message += " already exists";
				aiSkipped.incrementAndGet();
			}
		}
		finally {
			System.out.println(counter.incrementAndGet() + message);
		}
	}
}
