package ch.alice.o2.ccdb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Properties;

/**
 * Handle all interactions with a local file, including the metadata access (backed by a .properties file with the same base name as the referenced file)
 *
 * @author costing
 * @since 2017-09-21
 */
class LocalObjectWithVersion implements Comparable<LocalObjectWithVersion> {
	final long startTime;
	long endTime;
	private long createTime = -1;

	final File referenceFile;

	private Properties objectProperties = null;

	boolean taintedProperties = false;

	public LocalObjectWithVersion(final long startTime, final File entry) {
		this.startTime = startTime;
		this.endTime = entry.lastModified();
		this.referenceFile = entry;
	}

	public String getProperty(final String key) {
		loadProperties();

		return objectProperties.getProperty(key);
	}

	public String getProperty(final String key, final String defaultValue) {
		loadProperties();

		return objectProperties.getProperty(key, defaultValue);
	}

	/**
	 * @param key
	 * @param value
	 * @return <code>true</code> if the content was modified
	 */
	public boolean setProperty(final String key, final String value) {
		loadProperties();

		final Object oldValue = objectProperties.setProperty(key, value);

		final boolean changed = oldValue == null || !value.equals(oldValue.toString());

		taintedProperties = taintedProperties || changed;

		return changed;
	}

	public String getOriginalName() {
		return getProperty("OriginalFileName", referenceFile.getName());
	}

	public boolean matches(final Map<String, String> flagConstraints) {
		if (flagConstraints.isEmpty())
			return true;

		loadProperties();

		search: for (final Map.Entry<String, String> entry : flagConstraints.entrySet()) {
			final String key = entry.getKey().trim();
			final String value = entry.getValue().trim();

			final String metaValue = objectProperties.getProperty(key);

			if (metaValue != null) {
				if (!metaValue.equals(value))
					return false;

				continue;
			}

			// fall back to searching for the key in case-insensitive mode

			for (final Map.Entry<Object, Object> e : objectProperties.entrySet()) {
				final String otherKey = e.getKey().toString();

				if (otherKey.equalsIgnoreCase(key)) {
					if (e.getValue().toString().equals(value))
						return false;

					continue search;
				}
			}

			// the required key was not found even in case-insensitive mode
			return false;
		}

		// all required keys matched
		return true;
	}

	public boolean covers(final long referenceTime) {
		return this.startTime <= referenceTime && this.endTime >= referenceTime;
	}

	private void loadProperties() {
		if (objectProperties == null) {
			objectProperties = new Properties();

			try (FileReader reader = new FileReader(referenceFile.getAbsolutePath() + ".properties")) {
				objectProperties.load(reader);
			} catch (@SuppressWarnings("unused") final IOException e) {
				// .properties file is missing
			}
		}
	}

	void saveProperties(final String remoteAddress) throws IOException {
		if (objectProperties != null && taintedProperties) {
			objectProperties.setProperty("Last-Modified", String.valueOf(System.currentTimeMillis()));

			if (remoteAddress != null)
				objectProperties.setProperty("UpdatedFrom", remoteAddress);

			try (OutputStream os = new FileOutputStream(referenceFile.getPath() + ".properties")) {
				objectProperties.store(os, null);
			}
		}
	}

	public long getCreateTime() {
		if (createTime > 0)
			return createTime;

		try {
			createTime = Long.parseLong(getProperty("CreateTime"));
		} catch (@SuppressWarnings("unused") NumberFormatException | NullPointerException ignore) {
			// lacking better information assume the object was created when the interval starts
			return startTime;
		}

		return createTime;
	}

	@Override
	public int compareTo(final LocalObjectWithVersion o) {
		final long diff = o.getCreateTime() - this.getCreateTime();

		if (diff < 0)
			return -1;

		if (diff > 0)
			return 1;

		return 0;
	}

	public void setValidityLimit(final long endTime) {
		this.endTime = endTime;
		referenceFile.setLastModified(endTime);
		setProperty("ValidUntil", String.valueOf(endTime));
	}
}