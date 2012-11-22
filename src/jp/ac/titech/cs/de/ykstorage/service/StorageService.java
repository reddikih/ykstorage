package jp.ac.titech.cs.de.ykstorage.service;

public class StorageService {

	private StorageManager sm;

	private void init() {
		this.sm = new StorageManager();
	}

	public Object put(Object key, Object value) {
		return sm.put(key, value);
	}

	public Object get(Object key) {
		return sm.get(key);
	}
}
