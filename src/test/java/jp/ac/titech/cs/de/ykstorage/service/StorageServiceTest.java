package jp.ac.titech.cs.de.ykstorage.service;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;

import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.service.StorageService;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StorageServiceTest {
	
	private StorageService storage;
	
	@Before
	public void setUpClass() {
		this.storage = new StorageService();
	}

	@Test
	public void EndToEnd() {
		String key1 = "testKey1";
		String value1 = "testValue1";
		storage.put(key1, value1);
		assertEquals(value1, storage.get(key1));

		String key2 = "testKey2";
		assertFalse(value1.equals(storage.get(key2)));
	}
	
	@After
	public void teardown() {
		for(String path : Parameter.DATA_DISK_PATHS) {
			File f = new File(path);
			f.delete();
		}
	}

}
