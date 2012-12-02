package jp.ac.titech.cs.de.ykstorage.service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import jp.ac.titech.cs.de.ykstorage.service.cmm.CacheMemoryManager;

public class StorageManager {

	private CacheMemoryManager cmm;
	private DiskManager dm;
	
	private AtomicInteger seqNum;
	private Map<String, Integer> keyMap;

	public StorageManager(CacheMemoryManager cmm) {
		this.cmm = cmm;
		this.dm = new DiskManager();
		this.seqNum = new AtomicInteger(0);
		this.keyMap = new HashMap<String, Integer>();
	}

	public Object get(Object key) {
		return null;
	}

	public boolean put(Object key, byte[] value) {
		int keyNum = getKeySequenceNumber((String)key);
		if (cmm.put(keyNum, value)) {
			return true;
		} else {
			return false;
		}
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
}
