package ch.alice.o2.ccdb.servlets.formatters;

import java.io.PrintWriter;
import java.util.Date;
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
				"<table style='font-size:10px' border=1 cellspacing=0 cellpadding=2><thead><tr><th>ID</th><th>Valid from</th><th>Valid until</th><th>Initial validity limit</th><th>Created at</th><th>Last modified</th><th>MD5</th><th>File name</th><th>Content type</th><th>Size</th><th>Path</th><th>Metadata</th><th>Replicas</th></thead>\n");
	}

	@Override
	public void format(final PrintWriter writer, final SQLObject obj) {
		writer.print("<tr><td nowrap align=left>");
		writer.print(obj.id.toString());

		writer.print("</td><td nowrap align=right>");
		writer.print(obj.validFrom);
		writer.println("<br>");
		final Date dFrom = new Date(obj.validFrom);
		writer.print(Format.showDate(dFrom));

		writer.print("</td><td nowrap align=right>");
		writer.print(obj.validUntil);
		writer.println("<br>");
		final Date dUntil = new Date(obj.validUntil);
		writer.print(Format.showDate(dUntil));

		writer.print("</td><td align=right>");
		writer.print(obj.initialValidity);
		writer.println("<br>");
		final Date dInitial = new Date(obj.initialValidity);
		writer.print(Format.showDate(dInitial));

		writer.print("</td><td align=right>");
		writer.print(obj.createTime);
		writer.println("<br>");
		final Date dCreated = new Date(obj.createTime);
		writer.print(Format.showDate(dCreated));

		writer.print("</td><td align=right>");
		writer.print(obj.getLastModified());
		writer.println("<br>");
		final Date dLastModified = new Date(obj.getLastModified());
		writer.print(Format.showDate(dLastModified));

		writer.print("</td><td align=center nowrap>");
		writer.print(obj.md5);

		writer.print("</td><td align=right nowrap>");
		writer.print(Format.escHtml(obj.fileName));

		writer.print("</td><td align=right nowrap>");
		writer.print(Format.escHtml(obj.contentType));

		writer.print("</td><td align=right nowrap>");
		writer.print(obj.size);

		writer.print("</td><td align=left nowrap>");
		writer.print(Format.escHtml(obj.getPath()));

		writer.print("</td><td align=left><dl>");
		for (final Map.Entry<Integer, String> entry : obj.metadata.entrySet()) {
			writer.print("<dt>");
			writer.print(Format.escHtml(SQLObject.getMetadataString(entry.getKey())));
			writer.print("</dt><dd>");
			writer.print(Format.escHtml(entry.getValue()));
			writer.print("</dd>\n");
		}

		writer.print("</dl></td><td align=left><ul>");

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
		writer.write("<!DOCTYPE html><html>\n");
	}

	@Override
	public void subfoldersListingHeader(final PrintWriter writer) {
		writer.write("<br><br><table style='font-size:10px' border=1 cellspacing=0 cellpadding=2><thead><tr><th>Subfolder</th></tr>\n");
	}

	@Override
	public void subfoldersListing(final PrintWriter writer, final String path, final String url) {
		writer.write("<tr><td><a href='/browse/");
		writer.write(Format.escHtml(url));
		writer.write("'>");
		writer.write(Format.escHtml(path));
		writer.write("</a></td></tr>\n");
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
