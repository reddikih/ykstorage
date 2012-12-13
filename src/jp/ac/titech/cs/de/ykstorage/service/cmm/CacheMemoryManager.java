package jp.ac.titech.cs.de.ykstorage.service.cmm;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import jp.ac.titech.cs.de.ykstorage.service.Value;
import jp.ac.titech.cs.de.ykstorage.util.StorageLogger;

public class CacheMemoryManager {

	private Logger logger = StorageLogger.getLogger();

	private ByteBuffer memBuffer;

	private int max;
	private int limit;

	/**
	 * key: data key
	 * value: MemoryHeader object
	 */
	private Map<Integer, MemoryHeader> headerTable;

	public CacheMemoryManager(int max, double threshold) {
		if (threshold < 0 || threshold > 1.0)
			throw new IllegalArgumentException("threshold must be in range 0 to 1.0.");

		this.max = max;
		this.limit = (int)Math.floor(max * threshold);
		logger.info(String.format("cache memory max capacity : %d[Bytes]", this.max));
		logger.info(String.format("cache memory threshold    : %d[Bytes]", this.limit));

		this.memBuffer = ByteBuffer.allocateDirect(max);
		this.headerTable = new HashMap<Integer, MemoryHeader>();
	}

	public boolean put(int key, Value value) {
		int usage = memBuffer.capacity() - memBuffer.remaining();
		int requireSize = value.getValue().length;
		if (this.limit < usage + requireSize) {
			logger.info(String.format(
					"cache memory overflow. key id: %d, require size: %d[B], available: %d[B]",
					key, requireSize, memBuffer.remaining()));
			return false;
		}

		MemoryHeader header =
			new MemoryHeader(memBuffer.position(), requireSize);
		headerTable.put(key, header);
		memBuffer.put(value.getValue());

//		Value isExist = get(key);
//		if (Value.NULL.equals(isExist)) {
//			// In case of to put the new entry.
//			MemoryHeader header =
//				new MemoryHeader(memBuffer.position(), requireSize);
//			headerTable.put(key, header);
//			memBuffer.put(value.getValue());
//		} else {
//			// already store the corresponding value.
//			MemoryHeader header = headerTable.get(key);
//
//		}

		logger.info(String.format(
				"put on cache memory. key id: %d, val pos: %d, size: %d",
				key, header.getPosition(), requireSize));

		return true;
	}

	public Value get(int key) {
		MemoryHeader header = headerTable.get(key);
		if (header == null) {
			return Value.NULL;
		}
		byte[] byteVal = new byte[header.getSize()];
		int currentPos = memBuffer.position();
		memBuffer.position(header.getPosition());
		memBuffer.get(byteVal, 0, header.getSize());
		memBuffer.position(currentPos);

		Value value = new Value(byteVal);

		return value;
	}

	public Value delete(int key) {
		Value deleted = get(key);
		if (!Value.NULL.equals(deleted)) {
			headerTable.remove(key);
			logger.info(String.format("delete from cache memory. key id: %d", key));
		}
		return deleted;
	}
	
	public void compaction() {
		boolean isFirst = true;
		for (MemoryHeader header : headerTable.values()) {
			if (isFirst) {
				int oldPosition = header.getPosition();
				byte[] byteVal = new byte[header.getSize()];
				memBuffer.position(oldPosition);
				memBuffer.get(byteVal, 0, header.getSize());
				memBuffer.rewind();
				int newPosition = memBuffer.position();
				memBuffer.put(byteVal);
				header.setPosition(newPosition);
				logger.info(String.format(
						"migrated. fromPos: %d, toPos: %d, size: %d", 
						oldPosition, newPosition, header.getSize()));
				isFirst = false;
				continue;
			}
			int currentPosition = memBuffer.position();
			int oldPosition = header.getPosition();
			byte[] byteVal = new byte[header.getSize()];
			memBuffer.position(oldPosition);
			memBuffer.get(byteVal, 0, header.getSize());
			memBuffer.position(currentPosition);
			memBuffer.put(byteVal);
			header.setPosition(currentPosition);
			logger.info(String.format(
					"migrated. fromPos: %d, toPos: %d, size: %d", 
					oldPosition, currentPosition, header.getSize()));
		}
	}

	class MemoryHeader {

		private int position;
		private int size;

		public MemoryHeader(int position, int size) {
			this.position = position;
			this.size = size;
		}

		public int getPosition() {
			return position;
		}
		
		public void setPosition(int position) {
			this.position = position;
		}

		public int getSize() {
			return size;
		}
		
		public void setSize(int size) {
			this.size = size;
		}
	}
}
