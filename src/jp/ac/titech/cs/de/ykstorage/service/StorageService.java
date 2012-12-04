package jp.ac.titech.cs.de.ykstorage.service;

import jp.ac.titech.cs.de.ykstorage.service.cmm.CacheMemoryManager;

public class StorageService {

	private StorageManager sm;

	public StorageService() {
		init();
	}
	
	private void init() {
		int capacity = Parameter.CAPACITY_OF_CACHEMEMORY;
		double threshold = Parameter.MEMORY_THRESHOLD;
		CacheMemoryManager cmm = new CacheMemoryManager(capacity, threshold);
		
		String[] diskPaths = Parameter.DATA_DISK_PATHS;
		DiskManager dm = new DiskManager(diskPaths);

		this.sm = new StorageManager(cmm, dm);
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
