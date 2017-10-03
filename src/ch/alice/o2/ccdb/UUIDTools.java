package ch.alice.o2.ccdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

/**
 * Helper class to generate version 1 (time based) UUIDs. This class doesn't implement fully the object since the MAC address is hardcoded to a fixed value. Since for this project the objects are
 * generated on the server side and thus the MAC address would be that of the server, this field is anyway not relevant.
 *
 * @author costing
 * @since 2017-10-02
 */
public class UUIDTools {
	private static int clockSequence = getSelfProcessID();

	private static long lastTimestamp = System.nanoTime() / 100 + 122192928000000000L;

	private static long lastTimestamp2 = System.nanoTime() / 100 + 122192928000000000L;

	private static int selfProcessID = 0;

	private static final String PROC_SELF = "/proc/self";

	/**
	 * Get JVM's process ID
	 *
	 * @return the process id, if it can be determined, or <code>-1</code> if not
	 */
	public static final int getSelfProcessID() {
		if (selfProcessID != 0)
			return selfProcessID;

		try {
			// on Linux
			selfProcessID = Integer.parseInt((new File(PROC_SELF)).getCanonicalFile().getName());

			return selfProcessID;
		} catch (@SuppressWarnings("unused") final Throwable t) {
			// ignore
		}

		try {
			selfProcessID = Integer.parseInt(System.getProperty("pid"));

			return selfProcessID;
		} catch (@SuppressWarnings("unused") final Throwable t) {
			// ignore
		}

		try {
			final String s = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();

			selfProcessID = Integer.parseInt(s.substring(0, s.indexOf('@')));
		} catch (@SuppressWarnings("unused") final Throwable t) {
			// ignore
		}

		// selfProcessID = -1;

		return selfProcessID;
	}

	private static final byte[] macAddress = new byte[] { 0x11, 0x22, 0x33, 0x44, 0x55, 0x66 };

	/**
	 * @param referenceTime
	 *            timestamp to be encoded in the UUID
	 * @param address
	 *            client's address to encode in the UUID. Up to 6 bytes from it will be placed in the MAC address field. If not provided (or less than 6 bytes long, ie. IPv4) then a fixed value will
	 *            be used for the missing part.
	 * @return a time UUID with the reference time set to the reference time
	 */
	public static synchronized UUID generateTimeUUID(final long referenceTime, final byte[] address) {
		final long time = referenceTime * 10000 + 122192928000000000L;

		if (time <= lastTimestamp2 || time <= lastTimestamp) {
			clockSequence++;

			if (clockSequence >= 65535)
				clockSequence = 0;
		}

		lastTimestamp2 = time;

		final byte[] contents = new byte[16];

		int addressBytes = 0;

		if (address != null) {
			addressBytes = Math.min(address.length, 6);

			for (int i = 0; i < addressBytes; i++)
				contents[10 + i] = address[i];
		}

		for (int i = addressBytes; i < 6; i++)
			contents[10 + i] = macAddress[i];

		final int timeHi = (int) (time >>> 32);
		final int timeLo = (int) time;

		contents[0] = (byte) (timeLo >>> 24);
		contents[1] = (byte) (timeLo >>> 16);
		contents[2] = (byte) (timeLo >>> 8);
		contents[3] = (byte) (timeLo);

		contents[4] = (byte) (timeHi >>> 8);
		contents[5] = (byte) timeHi;
		contents[6] = (byte) (timeHi >>> 24);
		contents[7] = (byte) (timeHi >>> 16);

		contents[8] = (byte) (clockSequence >> 8);
		contents[9] = (byte) clockSequence;

		contents[6] &= (byte) 0x0F;
		contents[6] |= (byte) 0x10;

		contents[8] &= (byte) 0x3F;
		contents[8] |= (byte) 0x80;

		long msb = 0;
		long lsb = 0;

		for (int i = 0; i < 8; i++)
			msb = (msb << 8) | (contents[i] & 0xff);
		for (int i = 8; i < 16; i++)
			lsb = (lsb << 8) | (contents[i] & 0xff);

		return new UUID(msb, lsb);
	}

	/**
	 * @param uuid
	 * @return epoch time of this uuid
	 */
	public static final long epochTime(final UUID uuid) {
		return (uuid.timestamp() - 0x01b21dd213814000L) / 10000;
	}

	/**
	 * Get the MD5 checksum of a file
	 *
	 * @param f
	 * @return the MD5 checksum of the entire file
	 * @throws IOException
	 */
	public static String getMD5(final File f) throws IOException {
		if (f == null || !f.isFile() || !f.canRead())
			throw new IOException("Cannot read from this file: " + f);

		MessageDigest md;
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (final NoSuchAlgorithmException e1) {
			throw new IOException("Could not initialize MD5 digester", e1);
		}

		try (DigestInputStream dis = new DigestInputStream(new FileInputStream(f), md)) {
			final byte[] buff = new byte[10240];

			int cnt;

			do
				cnt = dis.read(buff);
			while (cnt == buff.length);

			final byte[] digest = md.digest();

			return String.format("%032x", new BigInteger(1, digest));
		} catch (final IOException ioe) {
			throw ioe;
		}
	}
}
