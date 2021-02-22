package ch.alice.o2.ccdb.servlets.formatters;

import java.io.IOException;
import java.io.PrintWriter;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import alien.test.cassandra.tomcat.Options;
import ch.alice.o2.ccdb.multicast.Blob;
import ch.alice.o2.ccdb.servlets.LocalObjectWithVersion;
import ch.alice.o2.ccdb.servlets.SQLObject;
import lazyj.Format;
import lazyj.Utils;

/**
 * @author costing
 * @since 2018-04-26
 */
class JSONFormatter implements SQLFormatter {

	private static final Set<String> IGNORED_MEMORY_HEADERS = Set.of("InitialValidityLimit", "Valid-Until", "OriginalFileName", "Last-Modified", "Valid-From", "Content-Type", "File-Size", "Created",
			"Content-MD5");

	private static final Set<String> QC_SHORTCUT = Set.of("path", "createTime", "lastModified");

	private final Set<String> fieldFilter;

	private final boolean hasFilter;

	private final boolean isQCShortcut;

	private final boolean NEW_LINES = Utils.stringToBool(Options.getOption("json.expanded", "false"), false);

	/**
	 * Old style keys, in short format. Turned on by default for now, they should be switched off after QCG adopts the new format. TODO
	 */
	private final boolean OLD_KEYS = Utils.stringToBool(Options.getOption("json.old.keys", "true"), true);

	/**
	 * New style keys, the same ones as the equivalent HTTP headers
	 */
	private final boolean NEW_KEYS = Utils.stringToBool(Options.getOption("json.old.keys", "true"), true) || !OLD_KEYS;

	/**
	 * Restrict the returned fields to the ones in this set. Can be <code>null</code> or empty to mean "all".
	 *
	 * @param fieldFilter
	 */
	JSONFormatter(final Set<String> fieldFilter) {
		this.fieldFilter = fieldFilter;
		this.hasFilter = fieldFilter != null && fieldFilter.size() > 0;

		if (fieldFilter != null && fieldFilter.size() == 3 && fieldFilter.containsAll(QC_SHORTCUT))
			isQCShortcut = true;
		else
			isQCShortcut = false;
	}

	@Override
	public void header(final PrintWriter writer) {
		writer.print("\"objects\":[\n");
	}

	private final void filterContent(final Map<String, Object> jsonContent) {
		if (hasFilter) {
			final Iterator<Map.Entry<String, Object>> it = jsonContent.entrySet().iterator();

			while (it.hasNext()) {
				final Map.Entry<String, Object> entry = it.next();

				if (!fieldFilter.contains(entry.getKey()))
					it.remove();
			}
		}
	}

	private void writeMap(final PrintWriter writer, final Map<String, Object> jsonContent) {
		if (NEW_LINES)
			writer.println(Format.toJSON(jsonContent, true));
		else
			writer.print(Format.toJSON(jsonContent, false));
	}

	@Override
	public void format(final PrintWriter writer, final SQLObject obj) {
		final Map<String, Object> jsonContent = new LinkedHashMap<>();

		jsonContent.put("path", obj.getPath());

		if (OLD_KEYS) {
			jsonContent.put("createTime", Long.valueOf(obj.createTime));
			jsonContent.put("lastModified", Long.valueOf(obj.getLastModified()));
		}

		if (NEW_KEYS) {
			jsonContent.put("Created", Long.valueOf(obj.createTime));
			jsonContent.put("Last-Modified", Long.valueOf(obj.getLastModified()));
		}

		if (isQCShortcut) {
			// quick exit if these are all the fields needed by QC
			writeMap(writer, jsonContent);
			return;
		}

		if (OLD_KEYS) {
			jsonContent.put("id", obj.id.toString());
			jsonContent.put("validFrom", Long.valueOf(obj.validFrom));
			jsonContent.put("validUntil", Long.valueOf(obj.validUntil));
			jsonContent.put("initialValidity", Long.valueOf(obj.initialValidity));
			jsonContent.put("MD5", obj.md5);
			jsonContent.put("fileName", obj.fileName);
			jsonContent.put("contentType", obj.contentType);
			jsonContent.put("size", Long.valueOf(obj.size));
		}

		if (NEW_KEYS) {
			jsonContent.put("ETag", "\"" + obj.id.toString() + "\"");
			jsonContent.put("Valid-From", Long.valueOf(obj.validFrom));
			jsonContent.put("Valid-Until", Long.valueOf(obj.validUntil));
			jsonContent.put("InitialValidityLimit", Long.valueOf(obj.initialValidity));
			jsonContent.put("Content-MD5", obj.md5);
			jsonContent.put("Content-Disposition", "inline;filename=\"" + obj.fileName + "\"");
			jsonContent.put("Content-Type", obj.contentType);
			jsonContent.put("Content-Length", Long.valueOf(obj.size));
		}

		if (obj.uploadedFrom != null)
			jsonContent.put("UploadedFrom", obj.uploadedFrom);

		if (!hasFilter || !jsonContent.keySet().containsAll(fieldFilter)) {
			for (final Map.Entry<Integer, String> entry : obj.metadata.entrySet())
				jsonContent.put(SQLObject.getMetadataString(entry.getKey()), entry.getValue());

			final ArrayList<String> replicas = new ArrayList<>();

			for (final Integer replica : obj.replicas)
				for (final String address : obj.getAddress(replica, null, false))
					replicas.add(address);

			jsonContent.put("replicas", replicas);
		}

		filterContent(jsonContent);

		writeMap(writer, jsonContent);
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
		writer.write(Format.escJSON(path));
		writer.write("\"");
	}

	@Override
	public void subfoldersListing(final PrintWriter writer, final String path, final String url, final long ownCount, final long ownSize, final long subfolderCount, final long subfolderSize) {
		final Map<String, Object> jsonContent = new LinkedHashMap<>();

		jsonContent.put("name", path);
		jsonContent.put("ownFiles", Long.valueOf(ownCount));
		jsonContent.put("ownSize", Long.valueOf(ownSize));
		jsonContent.put("filesInSubfolders", Long.valueOf(subfolderCount));
		jsonContent.put("sizeOfSubfolders", Long.valueOf(subfolderSize));

		writeMap(writer, jsonContent);
	}

	@Override
	public void subfoldersListingFooter(final PrintWriter writer, final long ownCount, final long ownSize) {
		writer.write("]\n");
	}

	@Override
	public void end(final PrintWriter writer) {
		writer.write("}");
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see ch.alice.o2.ccdb.servlets.formatters.SQLFormatter#format(java.io.PrintWriter, ch.alice.o2.ccdb.servlets.LocalObjectWithVersion)
	 */
	@Override
	public void format(final PrintWriter writer, final LocalObjectWithVersion obj) {
		final Map<String, Object> jsonContent = new LinkedHashMap<>();

		jsonContent.put("path", obj.getFolder());

		if (OLD_KEYS) {
			jsonContent.put("createTime", Long.valueOf(obj.getCreateTime()));
			jsonContent.put("lastModified", Long.valueOf(obj.getLastModified()));
		}

		if (NEW_KEYS || !OLD_KEYS) {
			jsonContent.put("Created", Long.valueOf(obj.getCreateTime()));
			jsonContent.put("Last-Modified", Long.valueOf(obj.getLastModified()));
		}

		if (isQCShortcut) {
			// quick exit if these are all the fields needed by QC
			writeMap(writer, jsonContent);
			return;
		}

		if (OLD_KEYS) {
			jsonContent.put("id", obj.getID());
			jsonContent.put("validFrom", Long.valueOf(obj.getStartTime()));
			jsonContent.put("validUntil", Long.valueOf(obj.getEndTime()));
			jsonContent.put("initialValidity", Long.valueOf(obj.getInitialValidity()));
			jsonContent.put("MD5", obj.getProperty("Content-MD5"));
			jsonContent.put("fileName", obj.getOriginalName());
			jsonContent.put("contentType", obj.getProperty("Content-Type", "application/octet-stream"));
			jsonContent.put("size", Long.valueOf(obj.getSize()));
		}

		if (NEW_KEYS) {
			jsonContent.put("ETag", "\"" + obj.getID() + "\"");
			jsonContent.put("Valid-From", Long.valueOf(obj.getStartTime()));
			jsonContent.put("Valid-Until", Long.valueOf(obj.getEndTime()));
			jsonContent.put("InitialValidityLimit", Long.valueOf(obj.getInitialValidity()));
			jsonContent.put("Content-MD5", obj.getProperty("Content-MD5"));
			jsonContent.put("Content-Disposition", "inline;filename=\"" + obj.getOriginalName() + "\"");
			jsonContent.put("Content-Type", obj.getProperty("Content-Type", "application/octet-stream"));
			jsonContent.put("Content-Length", Long.valueOf(obj.getSize()));
		}

		if (!hasFilter || !jsonContent.keySet().containsAll(fieldFilter)) {
			for (final Object key : obj.getUserPropertiesKeys())
				jsonContent.put(key.toString(), obj.getProperty(key.toString()));

			jsonContent.put("replicas", Arrays.asList(obj.getPath()));
		}

		filterContent(jsonContent);

		writeMap(writer, jsonContent);
	}

	@Override
	public void setExtendedReport(final boolean extendedReport) {
		// Extended reporting not implemented for JSON dump
	}

	@Override
	public void format(final PrintWriter writer, final Blob obj) {
		final Map<String, Object> jsonContent = new LinkedHashMap<>();

		jsonContent.put("path", obj.getKey());

		if (OLD_KEYS) {
			jsonContent.put("createTime", Long.valueOf(obj.getCreateTime()));
			jsonContent.put("lastModified", Long.valueOf(obj.getLastModified()));
		}

		if (NEW_KEYS) {
			jsonContent.put("Created", Long.valueOf(obj.getCreateTime()));
			jsonContent.put("Last-Modified", Long.valueOf(obj.getLastModified()));
		}

		if (isQCShortcut) {
			// quick exit if these are all the fields needed by QC
			writeMap(writer, jsonContent);
			return;
		}

		if (OLD_KEYS) {
			jsonContent.put("id", obj.getUuid().toString());
			jsonContent.put("validFrom", Long.valueOf(obj.getStartTime()));
			jsonContent.put("validUntil", Long.valueOf(obj.getEndTime()));
			jsonContent.put("initialValidity", Long.valueOf(obj.getInitialValidity()));
			jsonContent.put("MD5", obj.getProperty("Content-MD5"));
			jsonContent.put("fileName", obj.getOriginalName());
			jsonContent.put("contentType", obj.getProperty("Content-Type", "application/octet-stream"));
			jsonContent.put("size", Long.valueOf(obj.getSize()));
		}

		if (NEW_KEYS) {
			jsonContent.put("ETag", "\"" + obj.getUuid().toString() + "\"");
			jsonContent.put("Valid-From", Long.valueOf(obj.getStartTime()));
			jsonContent.put("Valid-Until", Long.valueOf(obj.getEndTime()));
			jsonContent.put("InitialValidityLimit", Long.valueOf(obj.getInitialValidity()));
			jsonContent.put("Content-MD5", obj.getProperty("Content-MD5"));
			jsonContent.put("Content-Disposition", "inline;filename=\"" + obj.getOriginalName() + "\"");
			jsonContent.put("Content-Type", obj.getProperty("Content-Type", "application/octet-stream"));
			jsonContent.put("Content-Length", Long.valueOf(obj.getSize()));
		}

		if (!hasFilter || !jsonContent.keySet().containsAll(fieldFilter)) {
			for (final String key : obj.getMetadataMap().keySet())
				if (!IGNORED_MEMORY_HEADERS.contains(key))
					jsonContent.put(key, obj.getProperty(key));

			boolean isComplete = false;

			try {
				if (obj.isComplete()) {
					isComplete = true;

					jsonContent.put("replicas", Arrays.asList("/download/" + obj.getUuid()));
				}
			}
			catch (@SuppressWarnings("unused") final IOException | NoSuchAlgorithmException e) {
				// ignore
			}

			if (!isComplete)
				jsonContent.put("incomplete", Boolean.TRUE);
		}

		filterContent(jsonContent);

		writeMap(writer, jsonContent);
	}

	@Override
	public String getContentType() {
		return "application/json";
	}
}
