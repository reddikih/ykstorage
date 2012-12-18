package test.jp.ac.titech.cs.de.ykstorage.service;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.io.File;
import java.util.Arrays;

import jp.ac.titech.cs.de.ykstorage.service.MAIDCacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.service.MAIDDataDiskManager;
import jp.ac.titech.cs.de.ykstorage.service.MAIDStorageManager;
import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.service.cmm.CacheMemoryManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MAIDStorageManagerTest {

	private MAIDStorageManager sm;

	@Before
	public void setUpClass() {
		int cmmMax = 10;
		double threshold = 1.0;
		CacheMemoryManager cmm = new CacheMemoryManager(cmmMax, threshold);
		
		MAIDCacheDiskManager cachedm = new MAIDCacheDiskManager(
				Parameter.CACHE_DISK_PATHS,
				Parameter.DATA_DISK_SAVE_FILE_PATH,
				Parameter.MOUNT_POINT_PATHS,
				Parameter.SPIN_DOWN_THRESHOLD);
		
		MAIDDataDiskManager datadm = new MAIDDataDiskManager(
				Parameter.DATA_DISK_PATHS,
				Parameter.DATA_DISK_SAVE_FILE_PATH,
				Parameter.MOUNT_POINT_PATHS,
				Parameter.SPIN_DOWN_THRESHOLD);

		this.sm = new MAIDStorageManager(cmm, cachedm, datadm);
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

		// write to disk due to cache overflow.
		String key3 = "key3";
		byte[] value3 = {7,8,9,10,11};
		assertThat(sm.put(key3, value3), is(true));
		assertThat((Arrays.equals(value3, sm.get(key3))), is(true));
	}

	@After
	public void teardown() {
		for(String path : Parameter.DISK_PATHS) {
			File f = new File(path);
			f.delete();
		}
	}

}
