package ch.alice.o2.ccdb.servlets;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.tomcat.util.http.fileupload.IOUtils;

import ch.alice.o2.ccdb.Options;
import lazyj.Format;
import utils.CachedThreadPool;

/**
 * Upload the newly created SQL object to other CCDB instances. Targets are expected to by proxy services to EPN farm, that
 * would in turn relay the messages on multicast in the cluster.
 *
 * Configuration key is "http.targets" (or env. variable HTTP_TARGETS). It is parsed as a list of base URLs to which the content would be uploaded.
 *
 * Since this is a blocking operation the actual sending is done on a few background threads, to decouple from the original upload.
 *
 * @author costing
 * @since 2019-11-05
 */
public class SQLtoHTTP implements SQLNotifier {

	private final List<URL> destinations = new LinkedList<>();

	private final ExecutorService asyncUploaders = new CachedThreadPool(4, 2, TimeUnit.MINUTES);

	private SQLtoHTTP() {
		final String httpNotifications = Options.getOption("http.targets", null);

		if (httpNotifications != null) {
			final StringTokenizer st = new StringTokenizer(httpNotifications, " \r\t\n\f,;");

			while (st.hasMoreTokens()) {
				final String url = st.nextToken();

				try {
					destinations.add(new URL(url));
				}
				catch (final Throwable t) {
					System.err.println("http.targets contains an invalid URL: " + url + " : " + t.getMessage());
				}
			}
		}
	}

	private static SQLtoHTTP instance = null;

	/**
	 * @return singleton
	 */
	public static synchronized SQLtoHTTP getInstance() {
		if (instance == null) {
			final SQLtoHTTP attempt = new SQLtoHTTP();

			if (attempt.destinations.size() > 0) {
				instance = attempt;

				System.err.println("Will send new objects to " + instance.destinations);
			}
		}

		return instance;
	}

	@Override
	public String toString() {
		return "SQLtoHTTP @ " + destinations;
	}

	@Override
	public void newObject(final SQLObject object) {
		// notify all UDP receivers of the new object
		for (final URL destination : destinations)
			asyncUploaders.submit(() -> upload(object, destination));
	}

	/**
	 * Upload an SQLObject to a target repository
	 * 
	 * @param object
	 *            what to upload
	 * @param target
	 *            base URL of the target repository
	 */
	public static void upload(final SQLObject object, final URL target) {
		try {
			final File localFile = object.getLocalFile(false);

			if (localFile == null)
				throw new IOException("Local file doesn't exist");

			String objectPath = object.getPath() + "/" + object.validFrom + "/" + object.validUntil + "/" + object.id;

			for (final Map.Entry<Integer, String> entry : object.metadata.entrySet())
				objectPath += "/" + Format.encode(SQLObject.getMetadataString(entry.getKey())) + "=" + Format.encode(entry.getValue());

			final URL url = new URL(target, objectPath);

			// System.err.println("Making connection to " + url);

			final HttpURLConnection http = (HttpURLConnection) url.openConnection();
			http.setRequestMethod("POST"); // PUT is another valid option
			http.setRequestProperty("Expect", "100-continue");
			http.setRequestProperty("Force-Upload", "true");

			http.setDoOutput(true);
			http.setDoInput(true);

			final String boundary = UUID.randomUUID().toString();
			final byte[] boundaryBytes = ("--" + boundary + "\r\n").getBytes(StandardCharsets.UTF_8);
			final byte[] finishBoundaryBytes = ("\r\n--" + boundary + "--").getBytes(StandardCharsets.UTF_8);
			http.setRequestProperty("Content-Type", "multipart/form-data; charset=UTF-8; boundary=" + boundary);

			try (ByteArrayOutputStream out = new ByteArrayOutputStream((int) object.size + 10240)) {
				out.write(boundaryBytes);

				final String partName = object.getProperty("partName", "blob");

				final String mpHeader = "Content-Disposition: form-data; name=\"" + partName + "\"; filename=\"" + object.fileName + "\"\r\n" + "Content-Length: " + object.size + "\r\n"
						+ "Content-Type: " + object.contentType + "\r\n" + "\r\n";

				out.write(mpHeader.getBytes(StandardCharsets.UTF_8));

				final byte[] multipartHeader = out.toByteArray();

				http.setFixedLengthStreamingMode(multipartHeader.length + localFile.length() + finishBoundaryBytes.length);

				try (OutputStream httpOut = http.getOutputStream()) {
					httpOut.write(multipartHeader);

					try (InputStream is = new FileInputStream(localFile)) {
						IOUtils.copy(is, httpOut);
					}

					httpOut.write(finishBoundaryBytes);

					httpOut.flush();

					String line;

					try (BufferedReader br = new BufferedReader(new InputStreamReader(http.getInputStream()))) {
						while ((line = br.readLine()) != null)
							System.err.println(line);
					}
				}
			}

		}
		catch (final IOException ioe) {
			System.err.println("Error notifying " + target + " of " + object.getPath() + "/" + object.id + " : " + ioe.getMessage());
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
