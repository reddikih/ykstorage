package test.jp.ac.titech.cs.de.ykstorage.service;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import jp.ac.titech.cs.de.ykstorage.service.DiskManager;
import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.service.StorageManager;
import jp.ac.titech.cs.de.ykstorage.service.cmm.CacheMemoryManager;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.experimental.runners.Enclosed;

@RunWith(Enclosed.class)
public class StorageManagerTest {
	
	static String key1 = "key1";
	static byte[] value1 = new byte[]{1,2,3};
	static String key2 = "key2";
	static byte[] value2 = new byte[]{4,5,6};
	static String key3 = "key3";
	static byte[] value3 = new byte[]{7,8,9,10,11,12};
	static String key4 = "key4";
	static byte[] value4 = new byte[]{1,2,3,4,5,6,7,8,9,10,11,12};
	static String key5 = "key5";
	static byte[] value5 = new byte[]{1,2,3,4,5};
	static String key6 = "key6";
	static byte[] value6 = new byte[]{6,7,8,9,10};
	static String key7 = "key7";
	static byte[] value7 = new byte[]{1,2,3,4,5,6,7,8,9,10,11,12,13,14};
	
	public static class Memory0Byte {
		StorageManager sm;
		
		@Before
		public void setUp() {
			long cmmMax = 0L;
			double threshold = 0.0;
			CacheMemoryManager cmm = new CacheMemoryManager(cmmMax, threshold);
			
			DiskManager dm = new DiskManager(
					Parameter.DATA_DISK_PATHS,
					Parameter.DATA_DISK_SAVE_FILE_PATH,
					Parameter.MOUNT_POINT_PATHS,
					Parameter.SPIN_DOWN_THRESHOLD);

			this.sm = new StorageManager(cmm, dm);
		}
		
		@Test
		public void putTest() {
			boolean actual = sm.put(key1, value1);
			assertThat(actual, is(true));
		}
		
		@Test
		public void getTest() {
			sm.put(key1, value1);
			
			byte[] actual = sm.get(key1);
			assertThat(actual, is(value1));
		}
	}
	
	public static class Memory10Byte {
		StorageManager sm;
		
		@Before
		public void setUp() {
			long cmmMax = 10L;
			double threshold = 1.0;
			CacheMemoryManager cmm = new CacheMemoryManager(cmmMax, threshold);
			
			DiskManager dm = new DiskManager(
					Parameter.DATA_DISK_PATHS,
					Parameter.DATA_DISK_SAVE_FILE_PATH,
					Parameter.MOUNT_POINT_PATHS,
					Parameter.SPIN_DOWN_THRESHOLD);

			this.sm = new StorageManager(cmm, dm);
		}
		
		@Test
		public void putTest() {
			boolean actual = sm.put(key1, value1);
			assertThat(actual, is(true));
			
			actual = sm.put(key2, value2);
			assertThat(actual, is(true));
		}
		
		@Test
		public void getTest() {
			sm.put(key1, value1);
			sm.put(key2, value2);
			
			byte[] actual = sm.get(key1);
			assertThat(actual, is(value1));
			
			actual = sm.get(key2);
			assertThat(actual, is(value2));
		}
		
		// Write key3 to cache memeory due to LRU algorithm.
		// In this case, replaced key is key1.
		// TODO We should check the key1 was replaced.
		@Test
		public void lruOnMemoryTest() {
			sm.put(key1, value1);
			sm.put(key2, value2);
			sm.put(key3, value3);
			
			byte[] actual = sm.get(key1);
			assertThat(actual, is(value1));
			
			actual = sm.get(key2);
			assertThat(actual, is(value2));
			
			actual = sm.get(key3);
			assertThat(actual, is(value3));
		}
		
		@Test
		public void lruOnMemoryTest2() {
			sm.put(key5, value5);
			sm.put(key6, value6);
			sm.put(key3, value3);
			
			byte[] actual = sm.get(key5);
			assertThat(actual, is(value5));
			
			actual = sm.get(key6);
			assertThat(actual, is(value6));
			
			actual = sm.get(key3);
			assertThat(actual, is(value3));
		}
		
		@Test
		public void largeSizePutTest() {
			boolean actual = sm.put(key4, value4);
			assertThat(actual, is(true));
		}
		
		@Test
		public void largeSizeGetTest() {
			sm.put(key4, value4);
			
			byte[] actual = sm.get(key4);
			assertThat(actual, is(value4));
		}
		
		@Test
		public void updateTheSameKeys() {
			sm.put(key1, value1);
			sm.put(key1, value2);
			
			byte[] actual = sm.get(key1);
			assertThat(actual, is(value2));
		}
		
		@Test
		public void updateTheSameKeys2() {
			sm.put(key4, value4);
			sm.put(key4, value7);
			
			byte[] actual = sm.get(key4);
			assertThat(actual, is(value7));
		}
	}
	
}
