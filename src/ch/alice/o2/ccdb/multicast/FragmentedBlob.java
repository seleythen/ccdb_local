package ch.alice.o2.ccdb.multicast;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

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
		final byte[] fragmentOffset_byte_array = Arrays.copyOfRange(serialisedFragmentedBlob, Utils.FRAGMENT_OFFSET_START_INDEX, Utils.FRAGMENT_OFFSET_START_INDEX + Utils.SIZE_OF_FRAGMENT_OFFSET);
		// Get the fragment Offset:
		this.fragmentOffset = Utils.intFromByteArray(fragmentOffset_byte_array);

		// Field 2: Packet type
		final byte[] packetType_byte_array = Arrays.copyOfRange(serialisedFragmentedBlob, Utils.PACKET_TYPE_START_INDEX, Utils.PACKET_TYPE_START_INDEX + Utils.SIZE_OF_PACKET_TYPE);
		this.packetType = packetType_byte_array[0];

		// Field 3: UUID
		final byte[] uuid_byte_array = Arrays.copyOfRange(serialisedFragmentedBlob, Utils.UUID_START_INDEX, Utils.UUID_START_INDEX + Utils.SIZE_OF_UUID);

		this.uuid = Utils.getUuid(uuid_byte_array);

		// Field 4: Blob Payload Length
		final byte[] blobDataLength_byte_array = Arrays.copyOfRange(serialisedFragmentedBlob, Utils.BLOB_PAYLOAD_LENGTH_START_INDEX,
				Utils.BLOB_PAYLOAD_LENGTH_START_INDEX + Utils.SIZE_OF_BLOB_PAYLOAD_LENGTH);

		// Get the blob payload length:
		this.blobDataLength = Utils.intFromByteArray(blobDataLength_byte_array);

		// Field 5: Key length
		final byte[] keyLength_byte_array = Arrays.copyOfRange(serialisedFragmentedBlob, Utils.KEY_LENGTH_START_INDEX, Utils.KEY_LENGTH_START_INDEX + Utils.SIZE_OF_KEY_LENGTH);

		// Get the key length:
		final short keyLength = Utils.shortFromByteArray(keyLength_byte_array);

		// Field 6: Payload checksum
		this.payloadChecksum = Arrays.copyOfRange(serialisedFragmentedBlob, Utils.PAYLOAD_CHECKSUM_START_INDEX, Utils.PAYLOAD_CHECKSUM_START_INDEX + Utils.SIZE_OF_PAYLOAD_CHECKSUM);

		// Field 7: Key
		final byte[] key_byte_array = Arrays.copyOfRange(serialisedFragmentedBlob, Utils.KEY_START_INDEX, Utils.KEY_START_INDEX + keyLength);

		this.key = new String(key_byte_array, StandardCharsets.UTF_8);

		// Field 8: Payload
		this.payload = Arrays.copyOfRange(serialisedFragmentedBlob, Utils.KEY_START_INDEX + keyLength, packetLength - Utils.SIZE_OF_PACKET_CHECKSUM);
	}

	int getFragmentOffset() {
		return this.fragmentOffset;
	}

	void setFragmentOffset(final int fragmentOffset) {
		this.fragmentOffset = fragmentOffset;
	}

	String getKey() {
		return this.key;
	}

	void setKey(final String key) {
		this.key = key;
	}

	UUID getUuid() {
		return this.uuid;
	}

	void setUuid(final UUID uuid) {
		this.uuid = uuid;
	}

	byte[] getPayloadChecksum() {
		return this.payloadChecksum;
	}

	void setPayloadChecksum(final byte[] payloadChecksum) {
		this.payloadChecksum = payloadChecksum;
	}

	int getblobDataLength() {
		return this.blobDataLength;
	}

	void setblobDataLength(final int blobDataLength) {
		this.blobDataLength = blobDataLength;
	}

	byte getPachetType() {
		return this.packetType;
	}

	void setPachetType(final byte packetType) {
		this.packetType = packetType;
	}

	byte[] getPayload() {
		return this.payload;
	}

	void setPayload(final byte[] payload) {
		this.payload = payload;
	}

	@Override
	public String toString() {
		String output = "";
		if (this.packetType == Blob.METADATA_CODE) {
			output += "Metadata ";
		}
		else
			if (this.packetType == Blob.DATA_CODE) {
				output += "Data ";
			}
			else
				if (this.packetType == Blob.SMALL_BLOB_CODE) {
					output += "Small Blob ";
				}
		output += "fragmentedBlob with \n";
		output += "fragmentOffset = " + Integer.toString(this.fragmentOffset) + "\n";
		output += "key = " + this.key + "\n";
		output += "uuid = " + this.uuid.toString() + "\n";
		output += "payloadChecksum = " + new String(this.payloadChecksum) + "\n";
		output += "payload = " + new String(this.payload) + "\n";
		output += "packetChecksum = " + new String(this.packetChecksum) + "\n";

		return output;
	}
}
