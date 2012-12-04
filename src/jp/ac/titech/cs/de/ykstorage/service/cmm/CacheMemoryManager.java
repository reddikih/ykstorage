package jp.ac.titech.cs.de.ykstorage.service.cmm;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import jp.ac.titech.cs.de.ykstorage.service.Value;

public class CacheMemoryManager {

	private ByteBuffer memBuffer;
	
	private int max;
	private int limit;
	
	private Map<Integer, MemoryHeader> memTable;
	
	public CacheMemoryManager(int max, double threshold) {
		if (threshold < 0 || threshold > 1.0)
			throw new IllegalArgumentException("threshold must be in range 0 to 1.0.");
		
		this.max = max;
		this.limit = (int)Math.floor(max * threshold);
		
		this.memBuffer = ByteBuffer.allocateDirect(max);
		this.memTable = new HashMap<Integer, MemoryHeader>();
	}
	
	public boolean put(int key, Value value) {
		int usage = memBuffer.capacity() - memBuffer.remaining();
		if (this.limit < usage + value.getValue().length)
			return false;
		
		MemoryHeader header = 
				new MemoryHeader(memBuffer.position(), value.getValue().length);
		memTable.put(key, header);
		memBuffer.put(value.getValue());
		
		return true;
	}
	
	public Value get(int key) {
		MemoryHeader header = memTable.get(key);
		if (header == null) {
			return Value.NULL;
		}
		byte[] byteVal = new byte[header.getSize()];
		int curPos = memBuffer.position();
		memBuffer.position(header.getPosition());
		memBuffer.get(byteVal, 0, header.getSize());
		memBuffer.position(curPos);
		
		Value value = new Value(byteVal);

		return value;
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

		public int getSize() {
			return size;
		}
	}
}
