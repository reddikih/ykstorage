package test.jp.ac.titech.cs.de.ykstorage.storage;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.io.File;
import java.util.Arrays;

import jp.ac.titech.cs.de.ykstorage.storage.OLDMAIDStorageManager;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.OLDMAIDCacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.MAIDCacheDiskStateManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.OLDMAIDDataDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.MAIDDataDiskStateManager;
import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.CacheMemoryManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OLDMAIDStorageManagerTest {

	private OLDMAIDStorageManager sm;

	@Before
	public void setUpClass() {
		int cmmMax = 10;
		double threshold = 1.0;
		CacheMemoryManager cmm = new CacheMemoryManager(cmmMax, threshold);
		
		MAIDCacheDiskStateManager cdsm = new MAIDCacheDiskStateManager(Parameter.MOUNT_POINT_PATHS, Parameter.CACHE_DISK_PATHS,
				Parameter.ACCESS_THRESHOLD, Parameter.ACCESS_INTERVAL, Parameter.RMI_URL,
				Parameter.IS_CACHEDISK, Parameter.NUMBER_OF_CACHE_DISKS, Parameter.NUMBER_OF_DATA_DISKS);
		
		OLDMAIDCacheDiskManager cachedm = new OLDMAIDCacheDiskManager(
				Parameter.CACHE_DISK_PATHS,
				Parameter.DATA_DISK_SAVE_FILE_PATH,
				Parameter.MOUNT_POINT_PATHS,
				Parameter.SPIN_DOWN_THRESHOLD,
				Parameter.CAPACITY_OF_CACHEDISK,
				cdsm);
		
		MAIDDataDiskStateManager ddsm = new MAIDDataDiskStateManager(Parameter.MOUNT_POINT_PATHS, Parameter.DATA_DISK_PATHS,
				Parameter.SPIN_DOWN_THRESHOLD, Parameter.SPINDOWN_INTERVAL, Parameter.RMI_URL,
				Parameter.IS_CACHEDISK, Parameter.NUMBER_OF_CACHE_DISKS, Parameter.NUMBER_OF_DATA_DISKS,
				Parameter.ACC);
		
		OLDMAIDDataDiskManager datadm = new OLDMAIDDataDiskManager(
				Parameter.DATA_DISK_PATHS,
				Parameter.DATA_DISK_SAVE_FILE_PATH,
				Parameter.MOUNT_POINT_PATHS,
				Parameter.SPIN_DOWN_THRESHOLD,
				ddsm);

		this.sm = new OLDMAIDStorageManager(cmm, cachedm, datadm);
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

		MAIDCacheDiskStateManager cdsm2 = new MAIDCacheDiskStateManager(Parameter.MOUNT_POINT_PATHS, Parameter.CACHE_DISK_PATHS,
				Parameter.ACCESS_THRESHOLD, Parameter.ACCESS_INTERVAL, Parameter.RMI_URL,
				Parameter.IS_CACHEDISK, Parameter.NUMBER_OF_CACHE_DISKS, Parameter.NUMBER_OF_DATA_DISKS);
		
		OLDMAIDCacheDiskManager cachedm2 = new OLDMAIDCacheDiskManager(
				Parameter.CACHE_DISK_PATHS,
				Parameter.DATA_DISK_SAVE_FILE_PATH,
				Parameter.MOUNT_POINT_PATHS,
				Parameter.SPIN_DOWN_THRESHOLD,
				Parameter.CAPACITY_OF_CACHEDISK,
				cdsm2);
		
		MAIDDataDiskStateManager ddsm2 = new MAIDDataDiskStateManager(Parameter.MOUNT_POINT_PATHS, Parameter.DATA_DISK_PATHS,
				Parameter.SPIN_DOWN_THRESHOLD, Parameter.SPINDOWN_INTERVAL, Parameter.RMI_URL,
				Parameter.IS_CACHEDISK, Parameter.NUMBER_OF_CACHE_DISKS, Parameter.NUMBER_OF_DATA_DISKS,
				Parameter.ACC);
		
		OLDMAIDDataDiskManager datadm2 = new OLDMAIDDataDiskManager(
				Parameter.DATA_DISK_PATHS,
				Parameter.DATA_DISK_SAVE_FILE_PATH,
				Parameter.MOUNT_POINT_PATHS,
				Parameter.SPIN_DOWN_THRESHOLD,
				ddsm2);

		OLDMAIDStorageManager sm2 = new OLDMAIDStorageManager(cmm2, cachedm2, datadm2);
		
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
		for(String path : Parameter.DISK_PATHS) {
			File f = new File(path);
			f.delete();
		}
	}

}
