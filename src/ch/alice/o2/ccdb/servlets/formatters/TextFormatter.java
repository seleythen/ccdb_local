package ch.alice.o2.ccdb.servlets.formatters;

import java.io.PrintWriter;

import ch.alice.o2.ccdb.servlets.SQLObject;

/**
 * @author costing
 * @since 2018-04-26
 */
public class TextFormatter implements SQLFormatter {

	@Override
	public void header(final PrintWriter writer) {
		// nothing
	}

	@Override
	public void format(final PrintWriter writer, final SQLObject obj) {
		writer.println(obj.toString());
	}

	@Override
	public void footer(final PrintWriter writer) {
		// nothing
	}

	@Override
	public void middle(final PrintWriter writer) {
		writer.println();
	}

	@Override
	public void start(final PrintWriter writer) {
		// nothing
	}

	@Override
	public void subfoldersListingHeader(final PrintWriter writer) {
		writer.println("\n\nSubfolders:\n");
	}

	@Override
	public void subfoldersListing(final PrintWriter writer, final String path) {
		writer.println(path);
	}

	@Override
	public void subfoldersListingFooter(final PrintWriter writer) {
		// nothing
	}

	@Override
	public void end(final PrintWriter writer) {
		// nothing
	}
}
