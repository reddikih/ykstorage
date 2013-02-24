package jp.ac.titech.cs.de.ykstorage.service.cmm;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
	private LinkedHashMap<Integer, MemoryHeader> headerTable;
	
	/**
	 * key: data key
	 * value: deleted value
	 */
	public Map<Integer, Value> deletedMap;

	public CacheMemoryManager(long max, double threshold) {
		if (threshold < 0 || threshold > 1.0)
			throw new IllegalArgumentException("threshold must be in range 0 to 1.0.");
		if (max < 0 || max > Integer.MAX_VALUE)
			throw new IllegalArgumentException("max must be in range 0 to Integer.MAX_VALUE.");

		this.max = (int)max;
		this.limit = (int)Math.floor(max * threshold);
		this.memBuffer = ByteBuffer.allocateDirect(this.max);
		
		logger.fine(String.format("cache memory max capacity : %d[Bytes]", this.max));
		logger.fine(String.format("cache memory threshold    : %d[Bytes]", this.limit));
		
		this.headerTable = new LinkedHashMap<Integer, MemoryHeader>();
		this.deletedMap = new HashMap<Integer, Value>();
	}

	public Value put(int key, Value value) {
		int usage = memBuffer.capacity() - memBuffer.remaining();
		int requireSize = value.getValue().length;
		
		assert(usage==memBuffer.position());
		
		if(this.limit < requireSize) {
			return Value.NULL;
		}
		
		while(this.limit < usage + requireSize) {
			boolean result = lru();
			assert(result!=false);
			usage = memBuffer.capacity() - memBuffer.remaining();
		}
		
		long thisTime = System.nanoTime();
		MemoryHeader header =
			new MemoryHeader(memBuffer.position(), requireSize);
		headerTable.put(key, header);
		memBuffer.put(value.getValue());

		logger.fine(String.format(
				"put on cache memory. key id: %d, val pos: %d, size: %d, time: %d",
				key, header.getPosition(), requireSize, thisTime));

		return value;
	}
	
	private boolean lru() {
		Value value = Value.NULL;
		Iterator<Integer> itr = headerTable.keySet().iterator();
		
		if(itr.hasNext()) {
			int key = itr.next();
			value = delete(key);
		}
		
		return !Value.NULL.equals(value);
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
		
		long thisTime = System.nanoTime();
		headerTable.put(key, headerTable.remove(key));
		logger.fine(String.format("get from cache memory. key id: %d, time: %d",
									key, thisTime));

		return value;
	}

	public Value delete(int key) {
		Value deleted = get(key);
		if (!Value.NULL.equals(deleted)) {
			headerTable.remove(key);
			
			logger.fine(String.format("delete from cache memory. key id: %d", key));
			
			boolean isFirst = true;
			if(headerTable.isEmpty()) {
				memBuffer.rewind();
			}
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
//					logger.fine(String.format(
//							"migrated. fromPos: %d, toPos: %d, size: %d",
//							oldPosition, newPosition, header.getSize()));
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
//				logger.fine(String.format(
//						"migrated. fromPos: %d, toPos: %d, size: %d",
//						oldPosition, currentPosition, header.getSize()));
			}
			
			deletedMap.put(key, deleted);
		}
		return deleted;
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
