package jp.ac.titech.cs.de.ykstorage.service;

import jp.ac.titech.cs.de.ykstorage.service.cmm.CacheMemoryManager;

public class StorageService {

	private StorageManager sm;

	public StorageService() {
		init();
	}
	
	private void init() {
		CacheMemoryManager cmm = new CacheMemoryManager(10, 1.0);
		this.sm = new StorageManager(cmm);
	}

	public boolean put(String key, String value) {
		byte[] byteVal = value.getBytes();
		return sm.put(key, byteVal);
	}

	public Object get(String key) {
		byte[] byteVal = sm.get(key);
		String value = new String(byteVal); //TODO set character code
		return value;
	}
}
