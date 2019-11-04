/**
 * myjava.com.github.dosarudaniel.gsoc provides the classes necessary to send/
 * receive multicast messages which contains Blob object with random length,
 * random content payload.
 */
package ch.alice.o2.ccdb.multicast;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.catalogue.GUIDUtils;
import alien.test.cassandra.tomcat.Options;
import ch.alice.o2.ccdb.multicast.Utils.Pair;
import ch.alice.o2.ccdb.servlets.LocalObjectWithVersion;
import ch.alice.o2.ccdb.servlets.SQLObject;

/**
 * Blob class - the structure of the object sent via multicast messages
 *
 * @author dosarudaniel@gmail.com
 * @since 2019-03-07
 *
 */
public class Blob implements Comparable<Blob> {
	private static final Logger logger = SingletonLogger.getLogger();

	/**
	 * UDP packet containing only metadata
	 */
	public final static byte METADATA_CODE = 0;

	/**
	 * UDP packet containing only data
	 */
	public final static byte DATA_CODE = 1;

	/**
	 * Entire object in one blob
	 */
	public final static byte SMALL_BLOB_CODE = 2;

	private final UUID uuid;
	private final String key;
	private byte[] payloadChecksum = null;
	private byte[] metadataChecksum = null;
	private byte[] metadata = null;
	private byte[] payload = null;

	private final List<Pair> metadataByteRanges = new Vector<>();
	private final List<Pair> payloadByteRanges = new Vector<>();

	/**
	 * Start of the validity interval
	 */
	public long startTime = 0;

	/**
	 * End of the validity interval
	 */
	public long endTime = 0;

	private long lastTouched = System.currentTimeMillis();

	private boolean complete = true;

	/**
	 * Parameterized constructor - creates a Blob object to be sent that contains a
	 * payload and a checksum. The checksum is the Utils.CHECKSUM_TYPE of the
	 * payload.
	 *
	 * @param payload - The data byte array
	 * @param metadata - The metadata byte array
	 * @param key - The key string
	 * @param uuid - The UUID of the Blob
	 *
	 * @throws NoSuchAlgorithmException
	 * @throws IOException
	 * @throws SecurityException
	 */
	public Blob(final byte[] metadata, final byte[] payload, final String key, final UUID uuid) throws NoSuchAlgorithmException, SecurityException, IOException {
		this.metadata = metadata;
		this.metadataChecksum = Utils.calculateChecksum(this.metadata);

		this.payload = payload;
		this.payloadChecksum = Utils.calculateChecksum(this.payload);

		this.key = key;
		this.uuid = uuid;

		this.metadataByteRanges.add(new Pair(0, this.metadata.length));
		this.payloadByteRanges.add(new Pair(0, this.payload.length));
	}

	/**
	 * Parameterized constructor - creates a Blob object to be sent that contains a
	 * payload and a checksum. The checksum is the Utils.CHECKSUM_TYPE of the
	 * payload.
	 *
	 * @param metadataMap - The metadata HaspMap
	 * @param payload - The data byte array
	 * @param key - The key string
	 * @param uuid - The UUID of the Blob
	 *
	 * @throws NoSuchAlgorithmException
	 * @throws IOException
	 * @throws SecurityException
	 */
	public Blob(final Map<String, String> metadataMap, final byte[] payload, final String key, final UUID uuid) throws NoSuchAlgorithmException, SecurityException, IOException {
		this.metadata = Utils.serializeMetadata(metadataMap);
		this.metadataChecksum = Utils.calculateChecksum(this.metadata);
		this.payload = payload;
		this.payloadChecksum = Utils.calculateChecksum(this.payload);
		this.key = key;
		this.uuid = uuid;
		this.metadataByteRanges.add(new Pair(0, this.metadata.length));
		this.payloadByteRanges.add(new Pair(0, this.payload.length));
	}

	/**
	 * Unparameterized constructor - creates an empty Blob object that receives
	 * fragmentedBlob objects and puts their content into the metadata or payload
	 * members via addFragmentedBlob method
	 *
	 *
	 * @param key - The key string
	 * @param uuid - The UUID of the Blob
	 * @throws SecurityException
	 */
	public Blob(final String key, final UUID uuid) {
		this.key = key;
		this.uuid = uuid;
	}

	/**
	 * Transformation of local object to Blob, for sending out
	 *
	 * @param ref
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 */
	public Blob(final LocalObjectWithVersion ref) throws IOException, NoSuchAlgorithmException {
		this.uuid = UUID.fromString(ref.getID());
		this.key = ref.getPath();

		this.startTime = ref.getStartTime();
		this.endTime = ref.getEndTime();

		cachedMetadataMap = new ConcurrentHashMap<>();

		for (final Object o : ref.getPropertiesKeys())
			cachedMetadataMap.put(o.toString(), ref.getProperty(o.toString()));

		cachedMetadataMap.put("Valid-Until", String.valueOf(this.endTime));
		cachedMetadataMap.put("Valid-From", String.valueOf(this.startTime));
		cachedMetadataMap.put("Created", String.valueOf(ref.getCreateTime()));

		payload = new byte[(int) ref.referenceFile.length()];

		try (RandomAccessFile input = new RandomAccessFile(ref.referenceFile, "r")) {
			input.read(payload, 0, payload.length);
		}

		this.payloadChecksum = Utils.calculateChecksum(this.payload);

		this.metadata = Utils.serializeMetadata(cachedMetadataMap);
		this.metadataChecksum = Utils.calculateChecksum(this.metadata);

		this.metadataByteRanges.add(new Pair(0, this.metadata.length));
		this.payloadByteRanges.add(new Pair(0, this.payload.length));

		setComplete(true);
	}

	/**
	 * Transformation from a SQLObject (with a local file replica) to Blob, for sending out
	 *
	 * @param ref
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 */
	public Blob(final SQLObject ref) throws IOException, NoSuchAlgorithmException {
		this.uuid = ref.id;
		this.key = ref.getPath();

		this.startTime = ref.validFrom;
		this.endTime = ref.validUntil;

		cachedMetadataMap = new ConcurrentHashMap<>();

		for (final Map.Entry<Integer, String> entry : ref.metadata.entrySet())
			cachedMetadataMap.put(SQLObject.getMetadataString(entry.getKey()), entry.getValue());

		cachedMetadataMap.put("Valid-Until", String.valueOf(this.endTime));
		cachedMetadataMap.put("Valid-From", String.valueOf(this.startTime));
		cachedMetadataMap.put("Created", String.valueOf(ref.createTime));

		if (ref.fileName != null)
			cachedMetadataMap.put("OriginalFileName", ref.fileName);

		if (ref.contentType != null)
			cachedMetadataMap.put("Content-Type", ref.contentType);

		if (ref.md5 != null)
			cachedMetadataMap.put("Content-MD5", ref.md5);

		final File localFile = ref.getLocalFile(false);

		if (localFile != null) {
			payload = new byte[(int) localFile.length()];

			try (RandomAccessFile input = new RandomAccessFile(localFile, "r")) {
				input.read(payload, 0, payload.length);
			}
		}
		else
			throw new IOException("Cannot locate a local file for " + ref.id);

		this.payloadChecksum = Utils.calculateChecksum(this.payload);

		this.metadata = Utils.serializeMetadata(cachedMetadataMap);
		this.metadataChecksum = Utils.calculateChecksum(this.metadata);

		this.metadataByteRanges.add(new Pair(0, this.metadata.length));
		this.payloadByteRanges.add(new Pair(0, this.payload.length));

		setComplete(true);
	}

	/**
	 * Send method - fragment (if necessary) and send the missingBlock from metadata
	 * or payload as packetType parameter specifies
	 *
	 * @param maxPayloadSize - the maximum payload supported by a fragmented packet
	 * @param missingBlock - the interval to be sent via multicast from metadata
	 *            or payload
	 * @param packetType - specify what kind of data is missing so that it
	 *            should be send: METADATA_CODE or DATA_CODE
	 * @param targetIp - Destination multicast IP
	 * @param port - Socket port number
	 *
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 */
	public void send(final int maxPayloadSize, final Pair missingBlock, final byte packetType, final String targetIp, final int port)
			throws IOException, NoSuchAlgorithmException {

		if (packetType == METADATA_CODE) {
			// fragment [missingBlock.first, missingBlock.second]
			// build packet
			// Utils.sendFragmentMulticast(packet, targetIp, port);
			final byte[] metadataToSend = new byte[missingBlock.second - missingBlock.first];
			System.arraycopy(this.metadata, missingBlock.first, metadataToSend, 0,
					missingBlock.second - missingBlock.first);
			/*
			 * fragment metadata
			 */
			final byte[] commonHeader = new byte[Utils.SIZE_OF_FRAGMENTED_BLOB_HEADER - Utils.SIZE_OF_FRAGMENT_OFFSET
					- Utils.SIZE_OF_PAYLOAD_CHECKSUM];
			// 2. 1 byte, packet type or flags
			commonHeader[0] = METADATA_CODE;
			// 3. 16 bytes, uuid
			System.arraycopy(Utils.getBytes(this.uuid), 0, commonHeader, Utils.SIZE_OF_PACKET_TYPE, Utils.SIZE_OF_UUID);
			// 4. 4 bytes, blob metadata Length
			System.arraycopy(Utils.intToByteArray(this.metadata.length), 0, commonHeader,
					Utils.SIZE_OF_PACKET_TYPE + Utils.SIZE_OF_UUID, Utils.SIZE_OF_BLOB_PAYLOAD_LENGTH);
			// 5. 2 bytes, keyLength
			System.arraycopy(Utils.shortToByteArray((short) this.key.getBytes().length), 0, commonHeader,
					Utils.SIZE_OF_PACKET_TYPE + Utils.SIZE_OF_UUID + Utils.SIZE_OF_BLOB_PAYLOAD_LENGTH,
					Utils.SIZE_OF_KEY_LENGTH);

			int indexMetadata = 0;

			while (indexMetadata < metadataToSend.length) {
				int maxPayloadSize_copy = maxPayloadSize;
				if (maxPayloadSize_copy + indexMetadata > metadataToSend.length) {
					maxPayloadSize_copy = metadataToSend.length - indexMetadata;
				}

				final byte[] packet = new byte[Utils.SIZE_OF_FRAGMENTED_BLOB_HEADER_AND_TRAILER + maxPayloadSize_copy
						+ this.key.getBytes().length];

				// 1. fragment offset
				System.arraycopy(Utils.intToByteArray(indexMetadata), 0, packet, Utils.FRAGMENT_OFFSET_START_INDEX,
						Utils.SIZE_OF_FRAGMENT_OFFSET);
				// Fields 2,3,4,5 from commonHeader:packet type, uuid, blob metadata Length,
				// keyLength
				System.arraycopy(commonHeader, 0, packet, Utils.PACKET_TYPE_START_INDEX, commonHeader.length);
				// payload checksum
				System.arraycopy(this.metadataChecksum, 0, packet, Utils.PAYLOAD_CHECKSUM_START_INDEX,
						Utils.SIZE_OF_PAYLOAD_CHECKSUM);
				// the key
				System.arraycopy(this.key.getBytes(), 0, packet, Utils.KEY_START_INDEX, this.key.getBytes().length);
				// the payload metadata
				System.arraycopy(metadataToSend, indexMetadata, packet,
						Utils.KEY_START_INDEX + this.key.getBytes().length, maxPayloadSize_copy);
				// the packet checksum
				System.arraycopy(
						Utils.calculateChecksum(
								Arrays.copyOfRange(packet, 0, packet.length - Utils.SIZE_OF_PACKET_CHECKSUM)),
						0, packet, Utils.KEY_START_INDEX + this.key.getBytes().length + maxPayloadSize_copy,
						Utils.SIZE_OF_PACKET_CHECKSUM);
				// send the metadata packet
				Utils.sendFragmentMulticast(packet, targetIp, port);

				indexMetadata = indexMetadata + maxPayloadSize;
			}

		}
		else
			if (packetType == DATA_CODE) {
				// fragment [missingBlock.first, missingBlock.second]
				// build packet
				// Utils.sendFragmentMulticast(packet, targetIp, port);
				final byte[] payloadToSend = new byte[missingBlock.second - missingBlock.first];
				System.arraycopy(this.payload, missingBlock.first, payloadToSend, 0,
						missingBlock.second - missingBlock.first);
				/*
				 * fragment metadata
				 */
				final byte[] commonHeader = new byte[Utils.SIZE_OF_FRAGMENTED_BLOB_HEADER - Utils.SIZE_OF_FRAGMENT_OFFSET
						- Utils.SIZE_OF_PAYLOAD_CHECKSUM];
				// 2. 1 byte, packet type or flags
				commonHeader[0] = DATA_CODE;
				// 3. 16 bytes, uuid
				System.arraycopy(Utils.getBytes(this.uuid), 0, commonHeader, Utils.SIZE_OF_PACKET_TYPE, Utils.SIZE_OF_UUID);
				// 4. 4 bytes, blob payload Length
				System.arraycopy(Utils.intToByteArray(this.payload.length), 0, commonHeader,
						Utils.SIZE_OF_PACKET_TYPE + Utils.SIZE_OF_UUID, Utils.SIZE_OF_BLOB_PAYLOAD_LENGTH);
				// 5. 2 bytes, keyLength
				System.arraycopy(Utils.shortToByteArray((short) this.key.getBytes().length), 0, commonHeader,
						Utils.SIZE_OF_PACKET_TYPE + Utils.SIZE_OF_UUID + Utils.SIZE_OF_BLOB_PAYLOAD_LENGTH,
						Utils.SIZE_OF_KEY_LENGTH);

				int indexPayload = 0;

				while (indexPayload < payloadToSend.length) {
					int maxPayloadSize_copy = maxPayloadSize;
					if (maxPayloadSize_copy + indexPayload > payloadToSend.length) {
						maxPayloadSize_copy = payloadToSend.length - indexPayload;
					}

					final byte[] packet = new byte[Utils.SIZE_OF_FRAGMENTED_BLOB_HEADER_AND_TRAILER + maxPayloadSize_copy
							+ this.key.getBytes().length];

					// 1. fragment offset
					System.arraycopy(Utils.intToByteArray(indexPayload), 0, packet, Utils.FRAGMENT_OFFSET_START_INDEX,
							Utils.SIZE_OF_FRAGMENT_OFFSET);
					// Fields 2,3,4,5 from commonHeader:packet type, uuid, blob metadata Length,
					// keyLength
					System.arraycopy(commonHeader, 0, packet, Utils.PACKET_TYPE_START_INDEX, commonHeader.length);
					// payload checksum
					System.arraycopy(this.payloadChecksum, 0, packet, Utils.PAYLOAD_CHECKSUM_START_INDEX,
							Utils.SIZE_OF_PAYLOAD_CHECKSUM);
					// the key
					System.arraycopy(this.key.getBytes(), 0, packet, Utils.KEY_START_INDEX, this.key.getBytes().length);
					// the payload metadata
					System.arraycopy(payloadToSend, indexPayload, packet,
							Utils.KEY_START_INDEX + this.key.getBytes().length, maxPayloadSize_copy);
					// the packet checksum
					System.arraycopy(
							Utils.calculateChecksum(
									Arrays.copyOfRange(packet, 0, packet.length - Utils.SIZE_OF_PACKET_CHECKSUM)),
							0, packet, Utils.KEY_START_INDEX + this.key.getBytes().length + maxPayloadSize_copy,
							Utils.SIZE_OF_PACKET_CHECKSUM);

					// send the metadata packet
					Utils.sendFragmentMulticast(packet, targetIp, port);

					indexPayload = indexPayload + maxPayloadSize;
				}
			}
			else {
				throw new IOException("Packet type not recognized!");
			}
	}

	/**
	 * Send method - fragments a blob into smaller serialized fragmentedBlobs and
	 * sends them via UDP multicast. Reads the maxPayloadSize from a file <--TODO
	 *
	 * @param targetIp - Destination multicast IP
	 * @param port - Socket port number
	 * @throws NoSuchAlgorithmException
	 * @throws IOException
	 */
	public void send(final String targetIp, final int port) throws NoSuchAlgorithmException, IOException {
		final int maxPayloadSize = Options.getIntOption("max.payload.size", 1200);

		if (maxPayloadSize > this.payload.length + this.metadata.length) {
			// no need to fragment the Blob
			final byte[] metadataAndPayload = new byte[this.payload.length + this.metadata.length];

			System.arraycopy(this.metadata, 0, metadataAndPayload, 0, this.metadata.length);
			System.arraycopy(this.payload, 0, metadataAndPayload, this.metadata.length, this.payload.length);

			final byte[] packet = new byte[Utils.SIZE_OF_FRAGMENTED_BLOB_HEADER_AND_TRAILER + metadataAndPayload.length
					+ this.key.getBytes().length];
			// Fill the fields:
			// 1. fragment offset will always be zero
			System.arraycopy(Utils.intToByteArray(0), 0, packet, Utils.FRAGMENT_OFFSET_START_INDEX,
					Utils.SIZE_OF_FRAGMENT_OFFSET);

			// 2. 1 byte, packet type
			packet[Utils.PACKET_TYPE_START_INDEX] = SMALL_BLOB_CODE;

			// 3. 16 bytes, uuid
			System.arraycopy(Utils.getBytes(this.uuid), 0, packet, Utils.UUID_START_INDEX, Utils.SIZE_OF_UUID);

			// 4. 4 bytes, blob payload + metadata Length
			System.arraycopy(Utils.intToByteArray(this.payload.length), 0, packet,
					Utils.BLOB_PAYLOAD_LENGTH_START_INDEX, Utils.SIZE_OF_BLOB_PAYLOAD_LENGTH);

			// 5. 2 bytes, keyLength
			System.arraycopy(Utils.shortToByteArray((short) this.key.getBytes().length), 0, packet,
					Utils.KEY_LENGTH_START_INDEX, Utils.SIZE_OF_KEY_LENGTH);

			// 6. payload checksum
			System.arraycopy(this.payloadChecksum, 0, packet, Utils.PAYLOAD_CHECKSUM_START_INDEX,
					Utils.SIZE_OF_PAYLOAD_CHECKSUM);

			// 7. the key
			System.arraycopy(this.key.getBytes(), 0, packet, Utils.KEY_START_INDEX, this.key.getBytes().length);

			// 8. the payload and metadata
			System.arraycopy(metadataAndPayload, 0, packet, Utils.KEY_START_INDEX + this.key.getBytes().length,
					metadataAndPayload.length);

			// 9. the packet checksum
			System.arraycopy(
					Utils.calculateChecksum(
							Arrays.copyOfRange(packet, 0, packet.length - Utils.SIZE_OF_PACKET_CHECKSUM)),
					0, packet, Utils.KEY_START_INDEX + this.key.getBytes().length + metadataAndPayload.length,
					Utils.SIZE_OF_PACKET_CHECKSUM);

			// send the metadata packet
			Utils.sendFragmentMulticast(packet, targetIp, port);
		}
		else {
			send(maxPayloadSize, new Pair(0, this.metadata.length), METADATA_CODE, targetIp, port);
			send(maxPayloadSize, new Pair(0, this.payload.length), DATA_CODE, targetIp, port);
		}
	}

	/**
	 * Set the <i>complete</i> flag
	 *
	 * @param newCompleteFlag
	 * @return the old value of the <i>complete</i> flag
	 */
	public boolean setComplete(final boolean newCompleteFlag) {
		final boolean oldComplete = complete;

		complete = newCompleteFlag;

		return oldComplete;
	}

	private boolean isCompleteRecalculate = true;

	/**
	 * At next {@link #isComplete()} call, do a full re-assessment of its status, otherwise the cached value could be used
	 */
	public void recomputeIsComplete() {
		isCompleteRecalculate = true;
	}

	/**
	 * isComplete method - checks if a Blob is completely received
	 *
	 * @return boolean true if the Blob is Complete
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 */
	public boolean isComplete() throws NoSuchAlgorithmException, IOException {
		if (!isCompleteRecalculate)
			return complete;

		isCompleteRecalculate = false;

		complete = false;

		if (this.metadata == null || this.payload == null) {
			return complete;
		}

		// Check byte ranges size:
		if (this.payloadByteRanges.size() != 1 || this.metadataByteRanges.size() != 1) {
			return complete;
		}

		// Check byte ranges metadata:
		if (this.metadataByteRanges.get(0).first != 0 || this.metadataByteRanges.get(0).second != this.metadata.length) {
			return complete;
		}

		// Check byte ranges payload:
		if (this.payloadByteRanges.get(0).first != 0 || this.payloadByteRanges.get(0).second != this.payload.length) {
			return complete;
		}

		// Verify checksums
		if (!Arrays.equals(this.payloadChecksum, Utils.calculateChecksum(this.payload))) {
			throw new IOException("Payload checksum failed");
		}

		if (!Arrays.equals(this.metadataChecksum, Utils.calculateChecksum(this.metadata))) {
			throw new IOException("Metadata checksum failed");
		}

		if (startTime <= 0)
			startTime = Long.parseLong(getProperty("Valid-From"));

		if (endTime <= 0)
			endTime = Long.parseLong(getProperty("Valid-Until"));

		complete = true;

		return complete;
	}

	private static void addPairToList(final List<Pair> list, final Pair newRange) {
		synchronized (list) {
			if (list.size() == 0) {
				list.add(newRange);
				return;
			}

			final int idx = Collections.binarySearch(list, newRange);

			if (idx < 0) {
				final int pos = -idx - 1;

				if (pos == list.size()) {
					// can we join with the last entry?

					final Pair existing = list.get(list.size() - 1);

					if (existing.second == newRange.first)
						existing.second = newRange.second;
					else
						list.add(newRange);
				}
				else {
					final Pair existing = list.get(pos);

					if (existing.first == newRange.second) {
						if (pos > 0 && list.get(pos - 1).second == newRange.first) {
							list.get(pos - 1).second = existing.second;
							list.remove(pos);
						}
						else
							existing.first = newRange.first;
					}
					else {
						if (existing.second == newRange.first) {
							if (pos < list.size() - 1 && list.get(pos + 1).first == newRange.second) {
								existing.second = list.get(pos + 1).second;
								list.remove(pos + 1);
							}
							else {
								existing.second = newRange.second;
							}
						}
						else {
							// can we join with the previous one?
							if (pos > 0 && list.get(pos - 1).second == newRange.first)
								list.get(pos - 1).second = newRange.second;
							else
								list.add(pos, newRange);
						}
					}
				}
			}
		}
	}

	/**
	 * Assemble a Blob by adding one FragmentedBlob to it
	 *
	 * @param fragmentedBlob
	 * @throws NoSuchAlgorithmException
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 */
	public synchronized void addFragmentedBlob(final FragmentedBlob fragmentedBlob) throws NoSuchAlgorithmException, UnsupportedEncodingException, IOException {
		touch();

		final byte[] fragmentedPayload = fragmentedBlob.getPayload();
		final int fragmentOffset = fragmentedBlob.getFragmentOffset();
		final Pair pair = new Pair(fragmentOffset, fragmentOffset + fragmentedPayload.length);
		if (fragmentedBlob.getPachetType() == DATA_CODE) {
			if (this.payload == null) {
				this.payload = new byte[fragmentedBlob.getblobDataLength()];
				this.payloadChecksum = fragmentedBlob.getPayloadChecksum();
			}

			if (this.payload.length != fragmentedBlob.getblobDataLength()) { // Another fragment
				throw new IOException("payload.length should have size = " + fragmentedBlob.getblobDataLength());
			}

			System.arraycopy(fragmentedPayload, 0, this.payload, fragmentOffset, fragmentedPayload.length);

			addPairToList(this.payloadByteRanges, pair);
		}
		else
			if (fragmentedBlob.getPachetType() == METADATA_CODE) {
				if (this.metadata == null) {
					this.metadata = new byte[fragmentedBlob.getblobDataLength()];
					this.metadataChecksum = fragmentedBlob.getPayloadChecksum(); // metadata == payload
				}
				if (this.metadata.length != fragmentedBlob.getblobDataLength()) { // Another fragment
					throw new IOException("metadata.length should have size = " + fragmentedBlob.getblobDataLength());
				}

				System.arraycopy(fragmentedPayload, 0, this.metadata, fragmentOffset, fragmentedPayload.length);

				addPairToList(this.metadataByteRanges, pair);
			}
			else
				if (fragmentedBlob.getPachetType() == SMALL_BLOB_CODE) {
					if (this.metadata == null && this.payload == null) {
						final int metadataLength = fragmentedPayload.length - fragmentedBlob.getblobDataLength();
						final int payloadLength = fragmentedBlob.getblobDataLength();
						this.metadata = new byte[metadataLength];
						this.payload = new byte[payloadLength];

						System.arraycopy(fragmentedPayload, 0, this.metadata, fragmentOffset, metadataLength);
						System.arraycopy(fragmentedPayload, metadataLength, this.payload, fragmentOffset, payloadLength);
						this.payloadChecksum = fragmentedBlob.getPayloadChecksum();
						this.metadataChecksum = Utils.calculateChecksum(this.metadata);
						this.payloadByteRanges.add(new Pair(0, payloadLength));
						this.metadataByteRanges.add(new Pair(0, metadataLength));
					}
					else {
						logger.log(Level.WARNING, "metadata and payload byte arrays should be null for an empty SMALL BLOB");
					}
				}
				else {
					throw new IOException("Packet type not recognized!");
				}
	}

	/**
	 * @return ranges of missing data blocks
	 */
	public ArrayList<Pair> getMissingPayloadBlocks() {
		if (this.payload == null) {
			System.err.println("No payload so far, have to ask for the entire content");
			return null;
		}

		final ArrayList<Pair> missingBlocks = new ArrayList<>();

		synchronized (payloadByteRanges) {
			final Pair first = payloadByteRanges.get(0);

			if (first.first > 0)
				missingBlocks.add(new Pair(0, first.first - 1));

			for (int i = 0; i < this.payloadByteRanges.size() - 1; i++) {
				if (this.payloadByteRanges.get(i).second != this.payloadByteRanges.get(i + 1).first) {
					missingBlocks.add(new Pair(this.payloadByteRanges.get(i).second, this.payloadByteRanges.get(i + 1).first - 1));
				}
			}

			final Pair last = payloadByteRanges.get(payloadByteRanges.size() - 1);

			if (last.second < payload.length)
				missingBlocks.add(new Pair(last.second, payload.length - 1));
		}

		return missingBlocks;
	}

	/**
	 * @param data
	 * @param missingBlock
	 */
	public void addByteRange(final byte[] data, final Pair missingBlock) {
		if (this.payload == null) {
			// getting the entire payload blob
			this.payload = new byte[data.length];
		}

		System.arraycopy(data, 0, this.payload, missingBlock.first, data.length);

		addPairToList(this.payloadByteRanges, missingBlock);
	}

	/**
	 * @return series name (path to object in CCDB)
	 */
	public String getKey() {
		return this.key;
	}

	/**
	 * @return unique object identifier
	 */
	public UUID getUuid() {
		return this.uuid;
	}

	private Map<String, String> cachedMetadataMap = null;

	/**
	 * @return complete metadata map
	 */
	public Map<String, String> getMetadataMap() {
		if (cachedMetadataMap == null)
			cachedMetadataMap = Utils.deserializeMetadata(this.metadata);

		return cachedMetadataMap;
	}

	/**
	 * @param metadataKey
	 * @return the metadata value for this key, if present
	 */
	public String getProperty(final String metadataKey) {
		return getMetadataMap().get(metadataKey);
	}

	/**
	 * @param metadataKey
	 * @param defaultValue
	 * @return the metadata value for this key if present, or the defaultValue if missing
	 */
	public String getProperty(final String metadataKey, final String defaultValue) {
		return getMetadataMap().getOrDefault(metadataKey, defaultValue);
	}

	/**
	 * Set a metadata key to a new value, serializing again the metadata blob
	 *
	 * @param metadataKey
	 * @param value
	 */
	public void setProperty(final String metadataKey, final String value) {
		getMetadataMap().put(metadataKey, value);

		try {
			this.metadata = Utils.serializeMetadata(cachedMetadataMap);
			this.metadataChecksum = Utils.calculateChecksum(this.metadata);

			this.metadataByteRanges.clear();
			this.metadataByteRanges.add(new Pair(0, metadata.length));
		}
		catch (@SuppressWarnings("unused") final IOException | NoSuchAlgorithmException e) {
			// ignore
		}
	}

	/**
	 * @return the file name as it was originally uploaded
	 */
	public String getOriginalName() {
		return getProperty("OriginalFileName", "na");
	}

	/**
	 * @return raw metadata memory content
	 */
	public byte[] getMetadata() {
		return this.metadata;
	}

	/**
	 * @param metadata set the metadata block
	 */
	public void setMetadata(final byte[] metadata) {
		this.metadata = metadata;
	}

	/**
	 * @return payload blob
	 */
	public byte[] getPayload() {
		return this.payload;
	}

	/**
	 * @param payload new payload blob
	 */
	public void setPayload(final byte[] payload) {
		this.payload = payload;

		try {
			this.payloadChecksum = Utils.calculateChecksum(payload);
		}
		catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		this.payloadByteRanges.clear();
		this.payloadByteRanges.add(new Pair(0, payload.length));
	}

	@Override
	public String toString() {
		String output = "";
		output += "Blob with \n";
		output += "\t key = " + this.key + ", uuid = " + this.uuid.toString() + "\n";
		output += "\t validity between " + this.startTime + " and " + this.endTime + "\n";
		output += "\t metadata = " + getMetadataMap() + "\n";
		output += "\t payload = " + payload.length + " bytes\n";

		return output;
	}

	@Override
	public boolean equals(final Object o) {
		// If the object is compared with itself then return true
		if (o == this) {
			return true;
		}

		/*
		 * Check if o is an instance of Complex or not "null instanceof [type]" also
		 * returns false
		 */
		if (!(o instanceof Blob)) {
			return false;
		}

		// typecast o to Complex so that we can compare data members
		final Blob blob = (Blob) o;

		// Verify payload
		if (!this.key.equals(blob.getKey())) {
			return false;
		}

		// Verify uuid
		if (!this.uuid.equals(blob.getUuid())) {
			return false;
		}

		// Verify payload
		if (!Arrays.equals(this.payload, blob.getPayload())) {
			return false;
		}

		// Verify metadata
		if (!Arrays.equals(this.metadata, blob.getMetadata())) {
			return false;
		}

		return true;
	}

	@Override
	public int hashCode() {
		return uuid.hashCode();
	}

	/**
	 * @return the absolute timestamp from which this object applies
	 */
	public long getStartTime() {
		return startTime;
	}

	/**
	 * @return the absolute timestamp from which this object applies
	 */
	public long getEndTime() {
		return endTime;
	}

	/**
	 * @return last modification timestamp
	 */
	public long getLastTouched() {
		return lastTouched;
	}

	/**
	 * Update the last internally modification time, i.e. when a new fragment is received
	 */
	public void touch() {
		lastTouched = System.currentTimeMillis();
	}

	private long createTime = 0;

	/**
	 * @return when the object was first uploaded. In epoch millis.
	 */
	public long getCreateTime() {
		if (createTime > 0)
			return createTime;

		try {
			createTime = Long.parseLong(getProperty("Created"));
		}
		catch (@SuppressWarnings("unused") NumberFormatException | NullPointerException ignore) {
			createTime = GUIDUtils.epochTime(uuid);
		}

		return createTime;
	}

	@Override
	public int compareTo(final Blob o) {
		final long diff = o.getCreateTime() - this.getCreateTime();

		if (diff < 0)
			return -1;

		if (diff > 0)
			return 1;

		return 0;
	}

	/**
	 * @param referenceTime
	 * @return <code>true</code> if the reference time falls between start time (inclusive) and end time (exclusive).
	 */
	public boolean covers(final long referenceTime) {
		return this.startTime <= referenceTime && getEndTime() > referenceTime;
	}

	/**
	 * @param flagConstraints
	 * @return <code>true</code> if this object matches the given constraints
	 */
	public boolean matches(final Map<String, String> flagConstraints) {
		if (flagConstraints.isEmpty())
			return true;

		search:
		for (final Map.Entry<String, String> entry : flagConstraints.entrySet()) {
			final String metadataKey = entry.getKey().trim();
			final String value = entry.getValue().trim();

			final String metaValue = getProperty(metadataKey);

			if (metaValue != null) {
				if (!metaValue.equals(value))
					return false;

				continue;
			}

			// fall back to searching for the key in case-insensitive mode

			for (final Map.Entry<String, String> e : getMetadataMap().entrySet()) {
				final String otherKey = e.getKey().toString();

				if (otherKey.equalsIgnoreCase(metadataKey)) {
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
}
