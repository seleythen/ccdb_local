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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import ch.alice.o2.ccdb.servlets.SQLObject;
import ch.alice.o2.ccdb.servlets.SQLtoHTTP;
import lazyj.Format;
import lazyj.Utils;

/**
 * @author costing
 * @since Nov 26, 2020
 */
public class Synchronization {
	/**
	 * @param args
	 */
	public static void main(final String[] args) {
		if (args.length != 3) {
			System.out.println("CCDB instances synchronization tool");
			System.out.println("Usage: ch.alice.o2.ccdb.Synchronization [source repository] [target repository] [path]\n");
			System.out.println("Example:");
			System.out.println("ch.alice.o2.ccdb.Synchronization http://ccdb-test.cern.ch:8080 http://localhost:8080 /qc/ITS");
			return;
		}

		final String sourceRepo = args[0].replaceAll("/+$", "");
		final String targetRepo = args[1].replaceAll("/+$", "");
		String path = args[2];

		if (!path.startsWith("/"))
			path = "/" + path;

		path = path.replaceAll("/+$", "");

		URL targetRepository;
		try {
			targetRepository = new URL(targetRepo);
		}
		catch (MalformedURLException e1) {
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
		catch (@SuppressWarnings("unused") ParserConfigurationException e) {
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
		catch (SAXException e) {
			System.err.println("Cannot parse the reply from " + targetXmlPath + " due to : " + e.getMessage());
			return;
		}

		final HashSet<String> existingIDs = new HashSet<>();

		final NodeList targetObjects = docTarget.getElementsByTagName("object");
		for (int i = 0; i < targetObjects.getLength(); i++) {
			String id = targetObjects.item(i).getAttributes().getNamedItem("id").getNodeValue();

			if (id.startsWith("uuid"))
				id = id.substring(4);

			existingIDs.add(id);
		}

		System.out.println("Target reports " + existingIDs.size() + " objects under " + path);

		DocumentBuilder dbSource;
		try {
			dbSource = dbf.newDocumentBuilder();
		}
		catch (@SuppressWarnings("unused") ParserConfigurationException e) {
			return;
		}

		final String sourceXmlPath = sourceRepo + "/browse" + path + "*?Accept=text/xml";

		Document docSource;
		try {
			docSource = dbSource.parse(sourceXmlPath);
		}
		catch (SAXException e) {
			System.err.println("Cannot parse the reply from " + targetXmlPath + " due to : " + e.getMessage());
			return;
		}
		catch (IOException ioe) {
			System.err.println("Could not connect to " + sourceRepo + " due to : " + ioe.getMessage());
			return;
		}

		final NodeList sourceObjects = docSource.getElementsByTagName("object");

		System.out.println("Source reports " + sourceObjects.getLength() + " objects under " + path);

		// docTarget.normalizeDocument();

		System.setProperty("http.targets", targetRepo);
		System.setProperty("file.repository.location", System.getProperty("user.dir") + "/temp");

		int iSynchronized = 0;
		int iSkipped = 0;
		int iError = 0;
		long lSynchronizedSize = 0;

		for (int i = 0; i < sourceObjects.getLength(); i++) {
			final Node n = sourceObjects.item(i);

			final NamedNodeMap attr = n.getAttributes();

			String id = attr.getNamedItem("id").getNodeValue();
			if (id.startsWith("uuid"))
				id = id.substring(4);

			System.out.print((i + 1) + "/" + sourceObjects.getLength() + ": " + id);

			if (!existingIDs.contains(id)) {
				System.out.print(" is missing");

				final String fileName = attr.getNamedItem("fileName").getNodeValue();

				final String sourcePath = attr.getNamedItem("path").getNodeValue();

				final String validFrom = attr.getNamedItem("validFrom").getNodeValue();

				final String sourceURL = sourceRepo + "/" + sourcePath + "/" + validFrom + "/" + id + "?HTTPOnly=true";

				final SQLObject toUpload = new SQLObject(null, sourcePath, UUID.fromString(id));

				final File localFile = toUpload.getLocalFile(true);

				try {
					toUpload.size = Long.parseLong(attr.getNamedItem("size").getNodeValue());

					System.out.print(", downloading (" + Format.size(toUpload.size) + ") ... ");

					final String downloadedIn = Utils.download(sourceURL, localFile.getAbsolutePath());

					if (downloadedIn == null) {
						System.err.println("Could not save to " + localFile.getAbsolutePath());
						iError++;
						continue;
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

						if (m.getNodeName().equals("metadata"))
							toUpload.setProperty(m.getAttributes().getNamedItem("key").getNodeValue(), m.getAttributes().getNamedItem("value").getNodeValue());
					}

					System.out.print("uploading ... ");

					SQLtoHTTP.upload(toUpload, targetRepository);

					iSynchronized++;
					lSynchronizedSize += toUpload.size;

					System.out.println("done");
				}
				catch (@SuppressWarnings("unused") final IOException ioe) {
					System.out.println("\nCould not download " + sourceURL + " to " + fileName);
					iError++;
				}
				finally {
					localFile.delete();
				}
			}
			else {
				System.out.println("already exists");
				iSkipped++;
			}
		}

		System.out.println("Skipped: " + iSkipped + " files, failed to get: " + iError + " files, synchronized: " + iSynchronized + " files (" + Format.size(lSynchronizedSize) + ")");
	}
}
