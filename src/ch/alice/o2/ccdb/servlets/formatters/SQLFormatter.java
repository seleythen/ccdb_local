package ch.alice.o2.ccdb.servlets.formatters;

import java.io.PrintWriter;

import ch.alice.o2.ccdb.servlets.SQLObject;

/**
 * @author costing
 * @since 2018-04-26
 */
public interface SQLFormatter {
	/**
	 * Start of document
	 * 
	 * @param writer
	 */
	public void start(PrintWriter writer);

	/**
	 * Start of object listing
	 * 
	 * @param writer
	 *            object to append to
	 */
	public void header(PrintWriter writer);

	/**
	 * Formatting one object at a time
	 * 
	 * @param writer
	 *            object to append to
	 * @param obj
	 *            current element to serialize
	 */
	public void format(PrintWriter writer, SQLObject obj);

	/**
	 * If there is more than one element in the list and there has to be a separator between consecutive elements, this is one separator
	 * 
	 * @param writer
	 *            object to append to
	 */
	public void middle(PrintWriter writer);

	/**
	 * End of the object listing
	 * 
	 * @param writer
	 *            object to append to
	 */
	public void footer(PrintWriter writer);

	/**
	 * Start of the subfolders listing
	 * 
	 * @param writer
	 */
	public void subfoldersListingHeader(PrintWriter writer);

	/**
	 * One subpath
	 * 
	 * @param writer
	 * @param path
	 */
	public void subfoldersListing(PrintWriter writer, String path);

	/**
	 * End of subfolders listing
	 * 
	 * @param writer
	 */
	public void subfoldersListingFooter(PrintWriter writer);

	/**
	 * End of document
	 * 
	 * @param writer
	 */
	public void end(PrintWriter writer);
}
