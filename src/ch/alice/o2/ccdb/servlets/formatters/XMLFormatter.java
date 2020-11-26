package ch.alice.o2.ccdb.servlets.formatters;

import java.io.IOException;
import java.io.PrintWriter;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import ch.alice.o2.ccdb.multicast.Blob;
import ch.alice.o2.ccdb.servlets.LocalObjectWithVersion;
import ch.alice.o2.ccdb.servlets.SQLObject;
import lazyj.Format;

/**
 * @author costing
 * @since 2018-04-26
 */
class XMLFormatter implements SQLFormatter {
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
		writer.print(Format.escHtml(obj.md5));

		writer.print("' fileName='");
		writer.print(Format.escHtml(obj.fileName));

		writer.print("' contentType='");
		writer.print(Format.escHtml(obj.contentType));

		writer.print("' size='");
		writer.print(obj.size);

		writer.print("'  path='");
		writer.print(Format.escHtml(obj.getPath()));

		writer.print("'>\n");

		for (final Map.Entry<Integer, String> entry : obj.metadata.entrySet()) {
			writer.print("  <metadata key='");
			writer.print(Format.escHtml(SQLObject.getMetadataString(entry.getKey())));
			writer.print("' value='");
			writer.print(Format.escHtml(entry.getValue()));
			writer.print("'/>\n");
		}

		for (final Integer replica : obj.replicas) {
			for (final String address : obj.getAddress(replica, null, false)) {
				writer.print("  <replica addr='");
				writer.print(Format.escHtml(address));
				writer.print("'/>\n");
			}
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
	public void start(final PrintWriter writer) {
		writer.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
		writer.println("<!DOCTYPE document [");
		writer.println("<!ELEMENT document (objects?, folders?)>");
		writer.println("<!ELEMENT objects (object*)>");
		writer.println("<!ELEMENT object (metadata*, replica*)>");

		writer.println("<!ATTLIST object");
		writer.println("  id ID #REQUIRED");
		writer.println("  validFrom CDATA #REQUIRED");
		writer.println("  validUntil CDATA #REQUIRED");
		writer.println("  initialValidity CDATA #IMPLIED");
		writer.println("  created CDATA #IMPLIED");
		writer.println("  lastModified CDATA #IMPLIED");
		writer.println("  md5 CDATA #IMPLIED");
		writer.println("  fileName CDATA #IMPLIED");
		writer.println("  contentType CDATA #IMPLIED");
		writer.println("  size CDATA #REQUIRED");
		writer.println("  path CDATA #REQUIRED");
		writer.println(">");

		writer.println("<!ELEMENT metadata EMPTY>");
		writer.println("<!ATTLIST metadata");
		writer.println("  key CDATA #REQUIRED");
		writer.println("  value CDATA #REQUIRED");
		writer.println(">");

		writer.println("<!ELEMENT replica EMPTY>");
		writer.println("<!ATTLIST replica");
		writer.println("  id CDATA #REQUIRED");
		writer.println("  addr CDATA #REQUIRED");
		writer.println(">");

		writer.println("<!ELEMENT folders (path*)>");

		writer.println("<!ELEMENT path EMPTY>");
		writer.println("<!ATTLIST path name CDATA #REQUIRED>");

		writer.println("]>");
		writer.println("<document>");
	}

	@Override
	public void subfoldersListingHeader(final PrintWriter writer) {
		writer.println("<folders>");
	}

	@Override
	public void subfoldersListing(final PrintWriter writer, final String path, final String url) {
		writer.print("<path name='");
		writer.print(Format.escHtml(path));
		writer.println("'/>");
	}

	@Override
	public void subfoldersListing(final PrintWriter writer, final String path, final String url, final long ownCount, final long ownSize, final long subfolderCount, final long subfolderSize) {
		writer.print("<path name='");
		writer.print(Format.escHtml(path));
		writer.print("' ownFiles='");
		writer.print(ownCount);
		writer.print("' ownSize='");
		writer.print(ownSize);
		writer.print("' filesInSubfolders='");
		writer.print(subfolderCount);
		writer.print("' sizeOfSubfolders='");
		writer.print(subfolderSize);
		writer.println("'/>");
	}

	@Override
	public void subfoldersListingFooter(final PrintWriter writer, final long ownCount, final long ownSize) {
		writer.println("</folders>");
	}

	@Override
	public void end(final PrintWriter writer) {
		writer.println("</document>");
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see ch.alice.o2.ccdb.servlets.formatters.SQLFormatter#format(java.io.PrintWriter, ch.alice.o2.ccdb.servlets.LocalObjectWithVersion)
	 */
	@Override
	public void format(final PrintWriter writer, final LocalObjectWithVersion obj) {
		writer.print("<object id='");
		writer.print(Format.escHtml(obj.getID()));

		writer.print("' validFrom='");
		writer.print(obj.getStartTime());

		writer.print("' validUntil='");
		writer.print(obj.getEndTime());

		writer.print("' initialValidity='");
		writer.print(obj.getInitialValidity());

		writer.print("' created='");
		writer.print(obj.getCreateTime());

		writer.print("' lastModified='");
		writer.print(obj.getLastModified());

		writer.print("' md5='");
		writer.print(Format.escHtml(obj.getProperty("Content-MD5")));

		writer.print("' fileName='");
		writer.print(Format.escHtml(obj.getOriginalName()));

		writer.print("' contentType='");
		writer.print(Format.escHtml(obj.getProperty("Content-Type", "application/octet-stream")));

		writer.print("' size='");
		writer.print(obj.getSize());

		writer.print("'  path='");
		writer.print(Format.escHtml(obj.getFolder()));

		writer.print("'>\n");

		for (final Object key : obj.getUserPropertiesKeys()) {
			writer.print("  <metadata key='");
			writer.print(Format.escHtml(key.toString()));
			writer.print("' value='");
			writer.print(Format.escHtml(obj.getProperty(key.toString())));
			writer.print("'/>\n");
		}

		writer.print("  <replica id='");
		writer.print(0);
		writer.print("' addr='");
		writer.print(Format.escHtml(obj.getPath()));
		writer.print("'/>\n");

		writer.print("</object>\n");
	}

	@Override
	public void setExtendedReport(final boolean extendedReport) {
		// Extended report not implemented for XML dump
	}

	@Override
	public void format(final PrintWriter writer, final Blob obj) {
		writer.print("<object id='");
		writer.print(Format.escHtml(obj.getUuid().toString()));

		writer.print("' validFrom='");
		writer.print(obj.getStartTime());

		writer.print("' validUntil='");
		writer.print(obj.getEndTime());

		writer.print("' initialValidity='");
		writer.print(obj.getInitialValidity());

		writer.print("' created='");
		writer.print(obj.getCreateTime());

		writer.print("' lastModified='");
		writer.print(obj.getLastModified());

		writer.print("' md5='");
		writer.print(Format.escHtml(obj.getProperty("Content-MD5")));

		writer.print("' fileName='");
		writer.print(Format.escHtml(obj.getOriginalName()));

		writer.print("' contentType='");
		writer.print(Format.escHtml(obj.getProperty("Content-Type", "application/octet-stream")));

		writer.print("' size='");
		writer.print(obj.getSize());

		writer.print("'  path='");
		writer.print(Format.escHtml(obj.getKey()));

		boolean isComplete = false;

		try {
			if (obj.isComplete()) {
				isComplete = true;
			}
		}
		catch (@SuppressWarnings("unused") final IOException | NoSuchAlgorithmException e) {
			// ignore
		}

		writer.print("' complete='");
		writer.print(isComplete ? "true" : "false");

		writer.print("'>\n");

		for (final String key : obj.getMetadataMap().keySet()) {
			writer.print("  <metadata key='");
			writer.print(Format.escHtml(key));
			writer.print("' value='");
			writer.print(Format.escHtml(obj.getProperty(key)));
			writer.print("'/>\n");
		}

		if (isComplete)
			writer.print("  <replica id='0' addr='/download/" + obj.getUuid() + "'/>\n");

		writer.print("</object>\n");
	}

	@Override
	public String getContentType() {
		return "text/xml";
	}
}
