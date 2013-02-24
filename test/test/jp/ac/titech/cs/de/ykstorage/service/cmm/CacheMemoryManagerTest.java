package test.jp.ac.titech.cs.de.ykstorage.service.cmm;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import jp.ac.titech.cs.de.ykstorage.service.Value;
import jp.ac.titech.cs.de.ykstorage.service.cmm.CacheMemoryManager;

import org.junit.runner.RunWith;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;

@RunWith(Enclosed.class)
public class CacheMemoryManagerTest {

	static int key1 = 1;
	static Value value1 = new Value(new byte[]{1,2,3});
	static int key2 = 2;
	static Value value2 = new Value(new byte[]{4,5,6});
	static int key3 = 3;
	static Value value3 = new Value(new byte[]{7,8,9});
	static int key4 = 4;
	static Value value4 = new Value(new byte[]{10,11,12});
	static int key5 = 5;
	static Value value5 = new Value(new byte[]{13,14,15,16,17,18});
	
	
	public static class InitializeThreshold {
		CacheMemoryManager cmm;
		
		@Test(expected=IllegalArgumentException.class)
		public void thresholdLessThan0() throws Exception {
			cmm = new CacheMemoryManager(10, -0.1);
		}
		
		@Test(expected=IllegalArgumentException.class)
		public void thresholdMoreThan1() throws Exception {
			cmm = new CacheMemoryManager(10, 1.1);
		}
	}
	
	public static class InitializeMemorySize {
		CacheMemoryManager cmm;
		
		@Test
		public void memory0Byte() {
			cmm = new CacheMemoryManager(0, 1.0);
			Value actual = cmm.put(key1, value1);
			assertThat(actual, is(Value.NULL));
		}
		
		@Test(expected=IllegalArgumentException.class)
		public void memoryIntegerMaxByte() {
			cmm = new CacheMemoryManager(Integer.MAX_VALUE + 1, 1.0);
		}
		
		@Test
		public void memory10Byte() {
			cmm = new CacheMemoryManager(10, 1.0);
			Value actual = cmm.put(key1, value1);
			assertThat(actual, is(value1));
		}
	}
	
	public static class InvalidKey {
		CacheMemoryManager cmm;
		
		@Before
		public void setUp() throws Exception {
			cmm = new CacheMemoryManager(10, 1.0);
			cmm.put(key1, value1);
		}
		
		@Test
		public void getNullObjectByInvalidKey() {
			Value actual = cmm.get(key2);
			assertThat(actual, is(Value.NULL));
		}
	}
	
	public static class IdempotentPut {
		CacheMemoryManager cmm;
		
		@Before
		public void setUp() throws Exception {
			cmm = new CacheMemoryManager(10, 1.0);
			cmm.put(key1, value1);
		}
		
		@Test
		public void Idempotent() {
			Value actual = cmm.get(key1);
			assertThat(actual, is(value1));
			
			actual = cmm.get(key1);
			assertThat(cmm.get(key1), is(value1));
			
			actual = cmm.get(key1);
			assertThat(cmm.get(key1), is(value1));
		}
	}
	
	public static class PutAndGet {
		CacheMemoryManager cmm;
		
		@Before
		public void setUp() throws Exception {
			cmm = new CacheMemoryManager(10, 1.0);
			cmm.put(key1, value1);
			cmm.put(key2, value2);
			cmm.put(key3, value3);
		}
		
		@Test
		public void getTest() {
			Value actual = cmm.get(key1);
			assertThat(actual, is(value1));
			
			actual = cmm.get(key2);
			assertThat(actual, is(value2));
			
			actual = cmm.get(key3);
			assertThat(actual, is(value3));
			
			actual = cmm.get(key1);
			assertThat(actual, is(value1));
		}
	}
	
	public static class LargeSizePut {
		CacheMemoryManager cmm;
		
		@Before
		public void setUp() throws Exception {
			cmm = new CacheMemoryManager(5, 1.0);
		}
		
		@Test
		public void largeSize() {
			cmm.put(key5, value5);
			Value actual = cmm.get(key5);
			assertThat(actual, is(Value.NULL));
		}
	}
	
	public static class UpdateTheSameKeys {
		CacheMemoryManager cmm;
		
		@Before
		public void setUp() throws Exception {
			cmm = new CacheMemoryManager(10, 1.0);
			cmm.put(key1, value1);
			cmm.put(key2, value2);
		}
		
		@Test
		public void updateKey1() {
			cmm.put(key1, value3);
			Value actual = cmm.get(key1);
			assertThat(actual, is(value3));
		}
		
		@Test
		public void updateKey2() {
			cmm.put(key2, value3);
			Value actual = cmm.get(key2);
			assertThat(actual, is(value3));
		}
	}
	
	public static class DeleteKey {
		CacheMemoryManager cmm;
		
		@Before
		public void setUp() throws Exception {
			cmm = new CacheMemoryManager(10, 1.0);
			cmm.put(key1, value1);
		}
		
		@Test
		public void delete() {
			cmm.delete(key1);
			
			Value actual = cmm.get(key1);
			assertThat(actual, is(Value.NULL));
		}
	}
	
	public static class LRUReplacement {
		CacheMemoryManager cmm;
		
		@Before
		public void setUp() throws Exception {
			cmm = new CacheMemoryManager(10, 1.0);
			cmm.put(key1, value1);
			cmm.put(key2, value2);
			cmm.put(key3, value3);
		}
		
		@Test(timeout = 100L)
		public void lru1() {
			cmm.put(key4, value4);
			
			Value actual = cmm.get(key1);
			assertThat(actual, is(Value.NULL));
			
			actual = cmm.get(key4);
			assertThat(actual, is(value4));
		}
		
		@Test(timeout = 100L)
		public void lru2() {
			cmm.put(key5, value5);
			
			Value actual = cmm.get(key1);
			assertThat(actual, is(Value.NULL));
			
			actual = cmm.get(key2);
			assertThat(actual, is(Value.NULL));
			
			actual = cmm.get(key5);
			assertThat(actual, is(value5));
		}
	}
	
	public static class DeletedMap {
		CacheMemoryManager cmm;
		
		@Before
		public void setUp() throws Exception {
			cmm = new CacheMemoryManager(10, 1.0);
			cmm.put(key1, value1);
			cmm.put(key2, value2);
			cmm.put(key3, value3);
		}
		
		@Test
		public void deletedMap1() {
			cmm.put(key4, value4);
			
			Value actual = cmm.deletedMap.get(key1);
			assertThat(actual, is(value1));
		}
		
		@Test
		public void deletedMap2() {
			cmm.put(key5, value5);
			
			Value actual = cmm.deletedMap.get(key1);
			assertThat(actual, is(value1));
			
			actual = cmm.deletedMap.get(key2);
			assertThat(actual, is(value2));
		}
	}

}
