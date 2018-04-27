package ch.alice.o2.ccdb.servlets.formatters;

import java.io.PrintWriter;
import java.util.Map;

import ch.alice.o2.ccdb.servlets.SQLObject;
import lazyj.Format;

/**
 * @author costing
 * @since 2018-04-26
 */
public class HTMLFormatter implements SQLFormatter {

	@Override
	public void header(final PrintWriter writer) {
		writer.print(
				"<html><table><thead><tr><th>ID</th><th>Valid from</th><th>Valid until</th><th>Initial validity limit</th><th>Created at</th><th>Last modified</th><th>MD5</th><th>File name</th><th>Content type</th><th>Metadata</th><th>Replicas</th></thead>\n");
	}

	@Override
	public void format(final PrintWriter writer, final SQLObject obj) {
		writer.print("<tr><td>");
		writer.print(obj.id.toString());

		writer.print("</td><td>");
		writer.print(obj.validFrom);

		writer.print("</td><td>");
		writer.print(obj.validUntil);

		writer.print("</td><td>");
		writer.print(obj.initialValidity);

		writer.print("</td><td>");
		writer.print(obj.createTime);

		writer.print("</td><td>");
		writer.print(obj.getLastModified());

		writer.print("</td><td>");
		writer.print(obj.md5);

		writer.print("</td><td>");
		writer.print(Format.escHtml(obj.fileName));

		writer.print("</td><td>");
		writer.print(Format.escHtml(obj.contentType));

		writer.print("</td><td><dl>");
		for (final Map.Entry<Integer, String> entry : obj.metadata.entrySet()) {
			writer.print("<dt>");
			writer.print(Format.escHtml(SQLObject.getMetadataString(entry.getKey())));
			writer.print("</dt><dd>");
			writer.print(Format.escHtml(entry.getValue()));
			writer.print("</dd>\n");
		}

		writer.print("</dl></td><td><ul>");

		for (final Integer replica : obj.replicas) {
			writer.print("<li><a href='");
			writer.print(Format.escHtml(obj.getAddress(replica)));
			writer.print("'>");
			writer.print(replica);
			writer.print("</a></li>\n");
		}

		writer.print("</ul></td></tr>\n");
	}

	@Override
	public void footer(final PrintWriter writer) {
		writer.print("</table>\n");
	}

	@Override
	public void middle(final PrintWriter writer) {
		// nothing
	}

	@Override
	public void start(final PrintWriter writer) {
		writer.write("<html>\n");
	}

	@Override
	public void subfoldersListingHeader(final PrintWriter writer) {
		writer.write("<table><thead><tr><th>Subfolder</th></tr>\n");
	}

	@Override
	public void subfoldersListing(final PrintWriter writer, final String path) {
		writer.write("<tr><td>");
		writer.write(Format.escHtml(path));
		writer.write("</td></tr>\n");
	}

	@Override
	public void subfoldersListingFooter(final PrintWriter writer) {
		writer.write("</table>\n");
	}

	@Override
	public void end(final PrintWriter writer) {
		writer.write("</html>");
	}
}
