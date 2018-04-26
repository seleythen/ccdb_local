package ch.alice.o2.ccdb.servlets.formatters;

import java.io.PrintWriter;
import java.util.Map;

import ch.alice.o2.ccdb.servlets.SQLObject;
import lazyj.Format;

/**
 * @author costing
 * @since 2018-04-26
 */
public class XMLFormatter implements SQLFormatter {

	@Override
	public void header(final PrintWriter writer) {
		writer.print("<objects>\n");
	}

	@Override
	public void format(final PrintWriter writer, final SQLObject obj) {
		writer.print("<object id='");
		writer.print(obj.id.toString());

		writer.print("' validFrom='");
		writer.print(obj.validFrom);

		writer.print("' validUntil='");
		writer.print(obj.validUntil);

		writer.print("' initialValidity='");
		writer.print(obj.initialValidity);

		writer.print("' created='");
		writer.print(obj.createTime);

		writer.print("' lastModified='");
		writer.print(obj.getLastModified());

		writer.print("' md5='");
		writer.print(obj.md5);

		writer.print("' fileName='");
		writer.print(Format.escHtml(obj.fileName));

		writer.print("' contentType='");
		writer.print(Format.escHtml(obj.contentType));

		writer.print("'>\n");

		for (final Map.Entry<Integer, String> entry : obj.metadata.entrySet()) {
			writer.print("  <metadata key='");
			writer.print(Format.escHtml(SQLObject.getMetadataString(entry.getKey())));
			writer.print("' value='");
			writer.print(Format.escHtml(entry.getValue()));
			writer.print("'/>\n");
		}

		for (final Integer replica : obj.replicas) {
			writer.print("  <replica id='");
			writer.print(replica);
			writer.print("' addr='");
			writer.print(Format.escHtml(obj.getAddress(replica)));
			writer.print("'/>\n");
		}

		writer.print("</object>\n");
	}

	@Override
	public void footer(final PrintWriter writer) {
		writer.print("</objects>\n");
	}

	@Override
	public void middle(final PrintWriter writer) {
		// nothing
	}

	@Override
	public void start(PrintWriter writer) {
		writer.println("<document>");
	}

	@Override
	public void subfoldersListingHeader(PrintWriter writer) {
		writer.println("<folders>");
	}

	@Override
	public void subfoldersListing(PrintWriter writer, String path) {
		writer.print("<path name='");
		writer.print(Format.escHtml(path));
		writer.println("'/>");
	}

	@Override
	public void subfoldersListingFooter(PrintWriter writer) {
		writer.println("</folders>");
	}

	@Override
	public void end(PrintWriter writer) {
		writer.println("</document>");
	}
}
