package jp.ac.titech.cs.de.ykstorage.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import jp.ac.titech.cs.de.ykstorage.service.Value;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.CacheMemoryManager;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.ReCacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.ReDataDiskManager;

public class ReStorageManager {
	private static final int CACHEMEM = 0;
	private static final int CACHEMEM_AND_DISK = 1;
	private static final int DATADISK = 2;
	private static final long TIMEOUT = 10000;
	
	private int numberOfData;
	private int dataCount = 0;
	
	private int numOfDisks;
	private int numOfCacheDisks;
	private long reInterval;
	
	private CacheMemoryManager cmm;
	private ReDataDiskManager datadm;
	private ReCacheDiskManager cachedm;

	private AtomicInteger seqNum;
	private Map<String, Integer> keyMap;
	
	/**
	 * key: from key
	 * value: to key
	 */
	private HashMap<Integer, Integer> removeKeyMap = new HashMap<Integer, Integer>();

	public ReStorageManager(CacheMemoryManager cmm, ReCacheDiskManager cachedm, ReDataDiskManager datadm, int numberOfData
			, int numOfDisks, int numOfCacheDisks, long reInterval) {
		this.cmm = cmm;
		this.datadm = datadm;
		this.cachedm = cachedm;
		this.numberOfData = numberOfData;
		this.seqNum = new AtomicInteger(0);
		this.keyMap = new HashMap<String, Integer>();
		this.numOfDisks = numOfDisks;
		this.numOfCacheDisks = numOfCacheDisks;
		this.reInterval = reInterval;
	}

	public byte[] get(String key) {
		int innerKey = getKeySequenceNumber(key);
		Value value = cmm.get(innerKey);
		
		if (Value.NULL.equals(value)) {
			value = cachedm.get(innerKey);
		}
		
		if (Value.NULL.equals(value)) {
			value = datadm.get(innerKey);
		}else {
			PutThread pt = new PutThread(CACHEMEM, innerKey, value);
			pt.start();
			return value.getValue();
		}
		
		// put value to cache disk.
//		if (!Value.NULL.equals(value)) {
//			PutThread pt = new PutThread(CACHEMEM_AND_DISK, innerKey, value);
//			pt.start();
//		}
		
		return value.getValue();
	}

	public boolean put(String key, byte[] bytes) {
		boolean cmmResult = true;
		boolean cacheResult = true;
		boolean dataResult = true;
		
		int keyNum = getKeySequenceNumber((String)key);
		int size = bytes.length;
		Value value = new Value(bytes);
		
		// メモリにValueを書き込む
		if (Value.NULL.equals(cmm.put(keyNum, value))) {
			if(hasCapacity(size)) {
				// LRU replacement on cache memory
				Set<Map.Entry<Integer, Value>> replaces = cmm.replace(keyNum, value);
//				for (Map.Entry<Integer, Value> replaced : replaces) {
//					// メモリから追い出されたValueはデータディスクに書き込む
//					if (!datadm.put(replaced.getKey(), replaced.getValue())) {
//						cmmResult = false;
//					}
//				}
			}
		}
		
		if(dataCount < numberOfData) {	// 初めのデータ配置はほぼ均等になるようにする
			if(dataCount % numOfDisks < numOfCacheDisks) {
				cacheResult = cachedm.put(keyNum, value);
			} else {
				dataResult = datadm.put(keyNum, value);
			}
			
			dataCount++;
		} else {
			cacheResult = cachedm.put(keyNum, value);
			
			if(!cacheResult) {
				dataResult = datadm.put(keyNum, value);
			}
		}
		
//		PutThread pt = new PutThread(DATADISK, keyNum, value);
//		pt.start();
//		
//		if(!(cmmResult & cacheResult)) {
//			try {
//				pt.join(TIMEOUT);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//				return false;
//			}
//			
//			if(pt.isAlive()) {
//				return false;
//			}
//		}
		
		return dataResult;
	}
	
	public void end() {
		datadm.end();
	}

	private int getKeySequenceNumber(String key) {
		int keySeqNum = -1;
		if (keyMap.containsKey(key)) {
			keySeqNum = keyMap.get(key);
		} else {
			keySeqNum = seqNum.getAndIncrement();
			keyMap.put(key, keySeqNum);
		}
		return keySeqNum;
	}
	
	private boolean hasCapacity(int size) {
		return this.cmm.hasCapacity(size);
	}
	
	class PutThread extends Thread {
		private int type;	// ->DiskManager dm;
		private int key;
		private Value value;
		
		public PutThread(int type, int key, Value value) {
			this.type = type;
			this.key = key;
			this.value = value;
		}
		
		public void run() {
			switch(type) {
			case CACHEMEM:
				// メモリにValueを書き込む
				if (Value.NULL.equals(cmm.put(key, value))) {
					if(hasCapacity(value.getValue().length)) {
						// LRU replacement on cache memory
						Set<Map.Entry<Integer, Value>> replaces = cmm.replace(key, value);
//						for (Map.Entry<Integer, Value> replaced : replaces) {
//							// メモリから追い出されたValueはデータディスクに書き込む
//							if (!datadm.put(replaced.getKey(), replaced.getValue())) {
//								cmmResult = false;
//							}
//						}
					}
				}
				break;
			case CACHEMEM_AND_DISK:
				// メモリにValueを書き込む
				if (Value.NULL.equals(cmm.put(key, value))) {
					if(hasCapacity(value.getValue().length)) {
						// LRU replacement on cache memory
						Set<Map.Entry<Integer, Value>> replaces = cmm.replace(key, value);
//						for (Map.Entry<Integer, Value> replaced : replaces) {
//							// メモリから追い出されたValueはデータディスクに書き込む
//							if (!datadm.put(replaced.getKey(), replaced.getValue())) {
//								cmmResult = false;
//							}
//						}
					}
				}
				cachedm.put(key, value);
				break;
			case DATADISK:
				datadm.put(key, value);
				break;
			}
		}
	}
	
	class CheckAccessThread extends Thread {
		
		public CheckAccessThread() {
			
		}
		
		public void run() {
			removeKeyMap = cachedm.checkAccess();
			
			try {
				Thread.sleep(reInterval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
}
