package ch.alice.o2.ccdb.multicast;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.catalogue.GUID;

/**
 * @author ddosaru
 *
 */
class FragmentedBlob {
	private static final Logger logger = SingletonLogger.getLogger();

	private int fragmentOffset;
	private byte packetType;
	private UUID uuid;
	// Total length of the Blob's payload if packetType is DATA or SMALL_BLOB
	// Total length of the Blob's metadata if packetType is METADATA
	private int blobDataLength;

	// private short keyLength; // <-- key.length()
	private byte[] payloadChecksum;
	private String key;
	private byte[] payload;
	private final byte[] packetChecksum;

	/**
	 * Manual deserialization of a serialisedFragmentedBlob
	 *
	 * @param serialisedFragmentedBlob
	 * @param packetLength
	 * @throws NoSuchAlgorithmException
	 * @throws IOException
	 *
	 */
	public FragmentedBlob(final byte[] serialisedFragmentedBlob, final int packetLength) throws NoSuchAlgorithmException, IOException {
		// Field 9: Packet Checksum
		this.packetChecksum = Arrays.copyOfRange(serialisedFragmentedBlob, packetLength - Utils.SIZE_OF_PACKET_CHECKSUM, packetLength);

		// Check packet checksum:
		if (!Arrays.equals(this.packetChecksum, Utils.calculateChecksum(Arrays.copyOfRange(serialisedFragmentedBlob, 0, packetLength - Utils.SIZE_OF_PACKET_CHECKSUM)))) {
			logger.log(Level.SEVERE, "Packet checksum failed!");
			throw new IOException("Packet checksum failed!");
		}

		// Field 1: Fragment Offset
		this.fragmentOffset = Utils.intFromByteArray(serialisedFragmentedBlob, Utils.FRAGMENT_OFFSET_START_INDEX);

		// Field 2: Packet type
		this.packetType = serialisedFragmentedBlob[Utils.PACKET_TYPE_START_INDEX];

		// Field 3: UUID
		final byte[] uuid_byte_array = Arrays.copyOfRange(serialisedFragmentedBlob, Utils.UUID_START_INDEX, Utils.UUID_START_INDEX + Utils.SIZE_OF_UUID);

		this.uuid = GUID.getUUID(uuid_byte_array);

		// Field 4: Blob Payload Length
		this.blobDataLength = Utils.intFromByteArray(serialisedFragmentedBlob, Utils.BLOB_PAYLOAD_LENGTH_START_INDEX);

		// Field 5: Key length
		final short keyLength = Utils.shortFromByteArray(serialisedFragmentedBlob, Utils.KEY_LENGTH_START_INDEX);

		// Field 6: Payload checksum
		this.payloadChecksum = Arrays.copyOfRange(serialisedFragmentedBlob, Utils.PAYLOAD_CHECKSUM_START_INDEX, Utils.PAYLOAD_CHECKSUM_START_INDEX + Utils.SIZE_OF_PAYLOAD_CHECKSUM);

		// Field 7: Key
		final byte[] key_byte_array = Arrays.copyOfRange(serialisedFragmentedBlob, Utils.KEY_START_INDEX, Utils.KEY_START_INDEX + keyLength);

		this.key = new String(key_byte_array, StandardCharsets.UTF_8);

		// Field 8: Payload
		this.payload = Arrays.copyOfRange(serialisedFragmentedBlob, Utils.KEY_START_INDEX + keyLength, packetLength - Utils.SIZE_OF_PACKET_CHECKSUM);
	}

	/**
	 * @return fragment offset within the block
	 */
	int getFragmentOffset() {
		return this.fragmentOffset;
	}

	/**
	 * @param fragmentOffset new offset value
	 */
	void setFragmentOffset(final int fragmentOffset) {
		this.fragmentOffset = fragmentOffset;
	}

	/**
	 * @return key
	 */
	String getKey() {
		return this.key;
	}

	/**
	 * @param key new key value
	 */
	void setKey(final String key) {
		this.key = key;
	}

	/**
	 * @return unique identifier of this object
	 */
	UUID getUuid() {
		return this.uuid;
	}

	/**
	 * @param uuid new uuid
	 */
	void setUuid(final UUID uuid) {
		this.uuid = uuid;
	}

	/**
	 * @return checksum
	 */
	byte[] getPayloadChecksum() {
		return this.payloadChecksum;
	}

	/**
	 * @param payloadChecksum new checksum value
	 */
	void setPayloadChecksum(final byte[] payloadChecksum) {
		this.payloadChecksum = payloadChecksum;
	}

	/**
	 * @return binary content length
	 */
	int getblobDataLength() {
		return this.blobDataLength;
	}

	/**
	 * @param blobDataLength setter
	 */
	void setblobDataLength(final int blobDataLength) {
		this.blobDataLength = blobDataLength;
	}

	/**
	 * @return packet type
	 */
	byte getPachetType() {
		return this.packetType;
	}

	/**
	 * @param packetType new value
	 */
	void setPachetType(final byte packetType) {
		this.packetType = packetType;
	}

	/**
	 * @return content
	 */
	byte[] getPayload() {
		return this.payload;
	}

	/**
	 * @param payload new content
	 */
	void setPayload(final byte[] payload) {
		this.payload = payload;
	}

	@Override
	public String toString() {
		final StringBuilder output = new StringBuilder();

		switch (this.packetType) {
			case Blob.METADATA_CODE:
				output.append("Metadata");
				break;
			case Blob.DATA_CODE:
				output.append("Data");
				break;
			default:
				output.append("Small Blob");
		}

		output.append(" fragmentedBlob with \nfragmentOffset = ").append(this.fragmentOffset);
		output.append("\nkey = ").append(this.key);
		output.append("\nuuid = ").append(this.uuid.toString());
		output.append("\npayloadChecksum = ").append(Utils.humanReadableChecksum(payloadChecksum));
		output.append("\npayload = ").append(payload.length).append(" bytes");
		output.append("\npacketChecksum = ").append(Utils.humanReadableChecksum(packetChecksum));

		return output.toString();
	}
}
