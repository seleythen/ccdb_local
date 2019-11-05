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
import java.util.StringTokenizer;
import java.util.UUID;

import org.apache.tomcat.util.http.fileupload.IOUtils;

import ch.alice.o2.ccdb.Options;
import lazyj.Format;

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

	static synchronized SQLtoHTTP getInstance() {
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
		for (final URL destination : destinations) {
			try {
				upload(object, destination);
			}
			catch (final IOException e) {
				System.err.println("Exception uploading to " + destination + " :" + e.getMessage());
				e.printStackTrace();
			}
		}
	}

	private static void upload(final SQLObject object, final URL target) throws IOException {
		final File localFile = object.getLocalFile(false);

		if (localFile == null)
			throw new IOException("Local file doesn't exist");

		final URL url = new URL(target, object.getPath() + "/" + object.validFrom + "/" + object.validUntil);

		System.err.println("Making connection to " + url);

		// TODO: add metadata keys

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

			final String mpHeader = "Content-Disposition: form-data; name=\"" + Format.encode(partName) + "\"; filename=\"" + Format.encode(object.fileName) + "\"\r\n" +
					"Content-Length: " + object.size + "\r\n" +
					"Content-Type: " + object.contentType + "\r\n" +
					"\r\n";

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

		System.err.println("Response code: " + http.getResponseCode());

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