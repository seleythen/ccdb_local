package ch.alice.o2.ccdb.multicast;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author ddosaru
 *
 */
public class Utils {
	/**
	 * Max UDP packet size that would be formed for sending out
	 */
	public final static int PACKET_MAX_SIZE = 65536;

	/**
	 * String conversion charset
	 */
	public final static String CHARSET = "UTF-8";

	/**
	 * Checksum algorithm
	 */
	public final static String CHECKSUM_TYPE = "MD5";

	// Field size (in bytes)
	/**
	 * Length of the fragment offset field, in bytes
	 */
	final static int SIZE_OF_FRAGMENT_OFFSET = 4;
	/**
	 * Length of packet type field
	 */
	final static int SIZE_OF_PACKET_TYPE = 1;
	/**
	 * UUID length
	 */
	final static int SIZE_OF_UUID = 16;
	/**
	 * Blob size, in bytes
	 */
	final static int SIZE_OF_BLOB_PAYLOAD_LENGTH = 4;
	/**
	 * Key cannot be longer than a UDP packet size, 2 bytes are enough
	 */
	final static int SIZE_OF_KEY_LENGTH = 2;
	/**
	 * MD5 checksum length
	 */
	final static int SIZE_OF_PAYLOAD_CHECKSUM = 16;
	/**
	 * MD5 checksum as well
	 */
	final static int SIZE_OF_PACKET_CHECKSUM = 16;

	/**
	 * Total header size
	 */
	final static int SIZE_OF_FRAGMENTED_BLOB_HEADER = SIZE_OF_FRAGMENT_OFFSET + SIZE_OF_PACKET_TYPE
			+ SIZE_OF_UUID + SIZE_OF_BLOB_PAYLOAD_LENGTH + SIZE_OF_KEY_LENGTH + SIZE_OF_PAYLOAD_CHECKSUM;

	/**
	 * One fragment, the above plus the packet checksum
	 */
	final static int SIZE_OF_FRAGMENTED_BLOB_HEADER_AND_TRAILER = SIZE_OF_FRAGMENTED_BLOB_HEADER
			+ SIZE_OF_PACKET_CHECKSUM;

	// Fragment Offset:-- 0 ........ 3
	// Packet Type: ----- 4
	// UUID: ------------ 5 ........ 20
	// blobPayloadLength: 21 ....... 24
	// keyLength:-------- 25 ....... 26
	// payloadChecksum:-- 27........ 42
	// key: ------------- 43 ....... 43+x-1
	// payload: --------- 43+x ..... 43+x+y-1
	// packetChecksum: -- 43+x+y ... 58+x+y

	// Start indexes of the fields in the serialized byte[]
	/**
	 * Precalculated offsets in the header
	 */
	final static int FRAGMENT_OFFSET_START_INDEX = 0;
	/**
	 * Where the packet type starts
	 */
	final static int PACKET_TYPE_START_INDEX = FRAGMENT_OFFSET_START_INDEX + SIZE_OF_FRAGMENT_OFFSET;
	/**
	 * Where the UUID field starts
	 */
	final static int UUID_START_INDEX = PACKET_TYPE_START_INDEX + SIZE_OF_PACKET_TYPE;
	/**
	 * Where the blob length starts
	 */
	final static int BLOB_PAYLOAD_LENGTH_START_INDEX = UUID_START_INDEX + SIZE_OF_UUID;
	/**
	 * Where the key length starts
	 */
	final static int KEY_LENGTH_START_INDEX = BLOB_PAYLOAD_LENGTH_START_INDEX + SIZE_OF_BLOB_PAYLOAD_LENGTH;
	/**
	 * Where the payload checksum starts
	 */
	final static int PAYLOAD_CHECKSUM_START_INDEX = KEY_LENGTH_START_INDEX + SIZE_OF_KEY_LENGTH;
	/**
	 * Where the key starts
	 */
	final static int KEY_START_INDEX = PAYLOAD_CHECKSUM_START_INDEX + SIZE_OF_PAYLOAD_CHECKSUM;
	// public final static int PAYLOAD_START_INDEX = KEY_START_INDEX + SIZE_OF_KEY
	// (unknown);
	// public final static int PACKET_CHECKSUM_START_INDEX = PAYLOAD_START_INDEX +
	// SIZE_OF_PAYLOAD (unknown);

	/**
	 * @author ddosaru
	 */
	public static class Pair implements Comparable<Pair> {
		/**
		 * Beginning of the interval, inclusive
		 */
		public int first;

		/**
		 * Interval end, exclusive
		 */
		public int second;

		/**
		 * @param first
		 * @param second
		 */
		public Pair(final int first, final int second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public String toString() {
			return "(" + Integer.toString(this.first) + "," + Integer.toString(this.second) + ")";
		}

		@Override
		public int compareTo(final Pair o) {
			return first - o.first;
		}

		@Override
		public boolean equals(final Object obj) {
			if (this == obj)
				return true;

			if (obj == null || !(obj instanceof Pair))
				return false;

			final Pair other = (Pair) obj;

			return this.first == other.first && this.second == other.second;
		}

		@Override
		public int hashCode() {
			return first * 43 + second * 23;
		}
	}

	/**
	 * Calculates the CHECKSUM_TYPE checksum of the byte[] data Default
	 * CHECKSUM_TYPE - MD5
	 *
	 * @param data
	 *
	 * @return byte[] - the checksum of the data
	 * @throws NoSuchAlgorithmException
	 */
	public static byte[] calculateChecksum(final byte[] data) throws NoSuchAlgorithmException {
		final MessageDigest mDigest = MessageDigest.getInstance(CHECKSUM_TYPE);
		mDigest.update(data);
		return mDigest.digest();
	}

	/**
	 * Sends multicast message that contains the serialized version of a
	 * fragmentedBlob
	 *
	 * @param packet - serialized fragmented Blob to send
	 * @param destinationIp - Destination IP address (multicast)
	 * @param destinationPort - Destination port number
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 */
	public static void sendFragmentMulticast(final byte[] packet, final String destinationIp, final int destinationPort) throws IOException, NoSuchAlgorithmException {
		try (DatagramSocket socket = new DatagramSocket()) {
			final InetAddress group = InetAddress.getByName(destinationIp);
			final DatagramPacket datagramPacket = new DatagramPacket(packet, packet.length, group, destinationPort);
			socket.send(datagramPacket);
		}
	}

	// Java has only signed data types, be aware of negatives values
	/**
	 * @param value
	 * @return serialization of an int in 4 bytes
	 */
	static final byte[] intToByteArray(final int value) {
		return new byte[] { (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value };
	}

	/**
	 * @param bytes
	 * @param offset
	 * @return the rebuilt int from 4 bytes
	 */
	static int intFromByteArray(final byte[] bytes, final int offset) {
		return bytes[offset] << 24 | (bytes[offset + 1] & 0xFF) << 16 | (bytes[offset + 2] & 0xFF) << 8 | (bytes[offset + 3] & 0xFF);
	}

	/**
	 * @param value
	 * @return serialized short in a 2-byte array
	 */
	static final byte[] shortToByteArray(final short value) {
		return new byte[] { (byte) (value >>> 8), (byte) value };
	}

	/**
	 * @param bytes
	 * @param offset
	 * @return short value rebuilt from 2 bytes
	 */
	static short shortFromByteArray(final byte[] bytes, final int offset) {
		return (short) ((bytes[offset] & 0xFF) << 8 | (bytes[offset + 1] & 0xFF));
	}

	/**
	 * Serializes a Map of metadata <key, value> into byte[]
	 *
	 * @param metadataMap - Map<String, String> with metadata key, values
	 * @return byte array with the serialized metadata
	 * @throws IOException
	 */
	public static byte[] serializeMetadata(final Map<String, String> metadataMap) throws IOException {
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			out.write(intToByteArray(metadataMap.size()));

			for (final Map.Entry<String, String> pair : metadataMap.entrySet()) {
				final byte[] key = pair.getKey().getBytes();
				final byte[] value = pair.getValue().getBytes();

				out.write(intToByteArray(key.length));
				out.write(key);
				out.write(intToByteArray(value.length));
				out.write(value);
			}

			// int size elements
			// -int keyLength
			// -String key
			// -int valueLength
			// -String value
			return out.toByteArray();
		}
	}

	/**
	 * Serializes a Map of metadata <key, value> into byte[]
	 *
	 * @param metadata - byte array with the serialized metadata
	 * @return metadata - Map<String, String> with metadata key, values
	 */
	public static Map<String, String> deserializeMetadata(final byte[] metadata) {
		if (metadata == null) {
			// return empty HashMap
			return new HashMap<>();
		}

		final int size = intFromByteArray(metadata, 0);
		int index = Integer.BYTES;

		final Map<String, String> metadataMap = new HashMap<>();

		for (int i = 0; i < size; i++) {
			final int keyLength = intFromByteArray(metadata, index);
			index += Integer.BYTES;

			final byte[] key_array = Arrays.copyOfRange(metadata, index, index + keyLength);
			index += keyLength;
			final String key = new String(key_array, StandardCharsets.UTF_8);

			final int valueLength = intFromByteArray(metadata, index);
			index += Integer.BYTES;

			final byte[] value_array = Arrays.copyOfRange(metadata, index, index + valueLength);
			index += valueLength;
			final String value = new String(value_array, StandardCharsets.UTF_8);

			metadataMap.put(key, value);
		}

		return metadataMap;
	}

	/**
	 * @param checksum
	 * @return the checksum in printable hex characters
	 */
	public static String humanReadableChecksum(final byte[] checksum) {
		return String.format("%032x", new BigInteger(1, checksum));
	}
}