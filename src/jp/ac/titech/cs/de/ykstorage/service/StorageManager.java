package jp.ac.titech.cs.de.ykstorage.service;

public class StorageManager {

	private CacheMemoryManager cmm;
	private DiskManager dm;

	public StorageManager() {
		this.cmm = new CacheMemoryManager();
		this.dm = new DiskManager();
	}

	public Object get(Object key) {
		return null;
	}

	public Object put(Object key, Object value) {
		return null;
	}
}
