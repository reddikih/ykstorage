package test.jp.ac.titech.cs.de.ykstorage.storage;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.io.File;
import java.util.Arrays;

import jp.ac.titech.cs.de.ykstorage.storage.DiskManager;
import jp.ac.titech.cs.de.ykstorage.service.Parameter;
<<<<<<< HEAD:test/test/jp/ac/titech/cs/de/ykstorage/storage/StorageManagerTest.java
import jp.ac.titech.cs.de.ykstorage.storage.OLDStorageManager;
=======
import jp.ac.titech.cs.de.ykstorage.storage.StorageManager;
>>>>>>> refactoring changed the packages:test/test/jp/ac/titech/cs/de/ykstorage/storage/StorageManagerTest.java
import jp.ac.titech.cs.de.ykstorage.storage.buffer.CacheMemoryManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StorageManagerTest {

	private OLDStorageManager sm;

	@Before
	public void setUpClass() {
		int cmmMax = 10;
		double threshold = 1.0;
		CacheMemoryManager cmm = new CacheMemoryManager(cmmMax, threshold);

		DiskManager dm = new DiskManager(
				Parameter.DATA_DISK_PATHS,
				Parameter.DATA_DISK_SAVE_FILE_PATH,
				Parameter.MOUNT_POINT_PATHS,
				Parameter.SPIN_DOWN_THRESHOLD);

		this.sm = new OLDStorageManager(cmm, dm);
	}

	@Test
	public void testPutAndGetOnCacheMemory() {
		String key1 = "key1";
		String key2 = "key2";
		byte[] value1 = {1,2,3};
		byte[] value2 = {4,5,6};
		sm.put(key1, value1);
		sm.put(key2, value2);
		assertTrue(Arrays.equals(value1, sm.get(key1)));
		assertTrue(Arrays.equals(value2, sm.get(key2)));

		// Write key3 to cache memeory due to LRU algorithm.
		// In this case, replaced key is key1.
		// TODO We should check the key1 was replaced.
		String key3 = "key3";
		byte[] value3 = {7,8,9,10,11};
		assertThat(sm.put(key3, value3), is(true));
		assertThat((Arrays.equals(value3, sm.get(key3))), is(true));
		assertThat((Arrays.equals(value2, sm.get(key2))), is(true));
		assertThat((Arrays.equals(value1, sm.get(key1))), is(true));
	}
	
	@Test
	public void zeroCapacityTest() {
		int cmmMax = 0;
		double threshold = 1.0;
		CacheMemoryManager cmm2 = new CacheMemoryManager(cmmMax, threshold);

		DiskManager dm2 = new DiskManager(
				Parameter.DATA_DISK_PATHS,
				Parameter.DATA_DISK_SAVE_FILE_PATH,
				Parameter.MOUNT_POINT_PATHS,
				Parameter.SPIN_DOWN_THRESHOLD);

		OLDStorageManager sm2 = new OLDStorageManager(cmm2, dm2);
		
		String key1 = "key1";
		byte[] value1 = {1,2,3};
		
		assertThat(sm2.put(key1, value1), is(true));
		assertThat(sm2.get(key1), is(value1));
	}
	
	@Test
	public void largeSizeTest() {
		String key1 = "key1";
		String key2 = "key2";
		byte[] value1 = {1,2,3};
		byte[] value2 = {1,2,3,4,5,6,7,8,9,0};
		
		assertThat(sm.put(key1, value1), is(true));
		assertThat(sm.put(key2, value2), is(true));
		assertThat(sm.get(key1), is(value1));
		assertThat(sm.get(key2), is(value2));
	}

	@After
	public void teardown() {
		for(String path : Parameter.DATA_DISK_PATHS) {
			File f = new File(path);
			f.delete();
		}
	}

}
