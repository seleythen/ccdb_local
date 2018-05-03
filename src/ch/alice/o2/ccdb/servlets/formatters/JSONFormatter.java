package ch.alice.o2.ccdb.servlets.formatters;

import java.io.PrintWriter;
import java.util.Map;

import ch.alice.o2.ccdb.servlets.SQLObject;
import lazyj.Format;

/**
 * @author costing
 * @since 2018-04-26
 */
public class JSONFormatter implements SQLFormatter {

	@Override
	public void header(final PrintWriter writer) {
		writer.print("\"objects\":[\n");
	}

	@Override
	public void format(final PrintWriter writer, final SQLObject obj) {
		writer.print("{\n  \"id\":\"");
		writer.print(obj.id.toString());

		writer.print("\",\n  \"validFrom\":\"");
		writer.print(obj.validFrom);

		writer.print("\",\n  \"validUntil\":\"");
		writer.print(obj.validUntil);

		writer.print("\",\n  \"initialValidity\":\"");
		writer.print(obj.initialValidity);

		writer.print("\",\n  \"createTime\":\"");
		writer.print(obj.createTime);

		writer.print("\",\n  \"lastModified\":\"");
		writer.print(obj.getLastModified());

		writer.print("\",\n  \"MD5\":\"");
		writer.print(obj.md5);

		writer.print("\",\n  \"fileName\":\"");
		writer.print(Format.escJS(obj.fileName));

		writer.print("\",\n  \"contentType\":\"");
		writer.print(Format.escJS(obj.contentType));

		writer.print("\",\n  \"size\":\"");
		writer.print(obj.size);

		writer.print("\"");
		for (final Map.Entry<Integer, String> entry : obj.metadata.entrySet()) {
			writer.print(",\n  \"");
			writer.print(Format.escJS(SQLObject.getMetadataString(entry.getKey())));
			writer.print("\":\"");
			writer.print(Format.escJS(entry.getValue()));
			writer.print("\"");
		}

		for (final Integer replica : obj.replicas) {
			writer.print(",\n  \"replica");
			writer.print(replica);
			writer.print("\":\"");
			writer.print(Format.escHtml(obj.getAddress(replica)));
			writer.print("\"");
		}

		writer.print("\n}");
	}

	@Override
	public void footer(final PrintWriter writer) {
		writer.print("]\n");
	}

	@Override
	public void middle(final PrintWriter writer) {
		writer.print(",\n");
	}

	@Override
	public void start(final PrintWriter writer) {
		writer.write("{");
	}

	@Override
	public void subfoldersListingHeader(final PrintWriter writer) {
		writer.write(",\"subfolders\":[\n");
	}

	@Override
	public void subfoldersListing(final PrintWriter writer, final String path, final String url) {
		writer.write("\"");
		writer.write(Format.escJS(path));
		writer.write("\"");
	}

	@Override
	public void subfoldersListingFooter(final PrintWriter writer) {
		writer.write("]\n");
	}

	@Override
	public void end(final PrintWriter writer) {
		writer.write("}");
	}
}
