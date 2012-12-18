package jp.ac.titech.cs.de.ykstorage.service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import jp.ac.titech.cs.de.ykstorage.service.cmm.CacheMemoryManager;

public class MAIDStorageManager {

	private CacheMemoryManager cmm;
	private MAIDDataDiskManager datadm;
	private MAIDCacheDiskManager cachedm;

	private AtomicInteger seqNum;
	private Map<String, Integer> keyMap;

	public MAIDStorageManager(CacheMemoryManager cmm, MAIDCacheDiskManager cachedm, MAIDDataDiskManager datadm) {
		this.cmm = cmm;
		this.datadm = datadm;
		this.cachedm = cachedm;
		this.seqNum = new AtomicInteger(0);
		this.keyMap = new HashMap<String, Integer>();
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
			return value.getValue();
		}
		
		// Thread put
		if (!Value.NULL.equals(value)) {
			PutThread pt = new PutThread(false, innerKey, value);
			pt.start();
		}
		
		return value.getValue();
	}

	public boolean put(String key, byte[] bytes) {
		int keyNum = getKeySequenceNumber((String)key);
		Value value = new Value(bytes);
		
		boolean cmmResult = cmm.put(keyNum, value);
		boolean cachedmResult = cachedm.put(keyNum, value);
		
		PutThread pt = new PutThread(true, keyNum, value);
		pt.start();
		
		return cachedmResult;
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
	
	class PutThread extends Thread {
		private boolean isDatadm;	// ->DiskManager dm;
		private int key;
		private Value value;
		
		public PutThread(boolean isDatadm, int key, Value value) {
			this.isDatadm = isDatadm;
			this.key = key;
			this.value = value;
		}
		
		public void run() {
			if(isDatadm) {
				datadm.put(key, value);
			}else {
				cachedm.put(key, value);
			}
		}
	}
	
}
