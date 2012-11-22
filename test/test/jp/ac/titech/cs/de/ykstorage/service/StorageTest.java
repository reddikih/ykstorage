package test.jp.ac.titech.cs.de.ykstorage.service;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import jp.ac.titech.cs.de.ykstorage.service.StorageService;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StorageTest {

	@Test
	public void EndToEnd() {
		StorageService storage = new StorageService();
		String key1 = "testKey1";
		String value1 = "testValue1";
		storage.put(key1, value1);
		assertEquals(value1, storage.get(key1));

		String key2 = "testKey2";
		String value2 = "testValue2";
		assertFalse(value1.equals(storage.get(key2)));
	}

}
