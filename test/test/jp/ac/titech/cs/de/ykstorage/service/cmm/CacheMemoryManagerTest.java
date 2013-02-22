package test.jp.ac.titech.cs.de.ykstorage.service.cmm;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import java.util.Map;
import java.util.Set;

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
	
	private CacheMemoryManager cmm;

	@Before
	public void setUpClass() {
		//TODO we can use parameterized tests.
		this.cmm = new CacheMemoryManager(10, 1.0);
	}

	@Test
	public void updateTheSameKeys() {
		int key1 = 1;
		Value value1 = new Value(new byte[]{1,2,3});
		cmm.put(key1, value1);
		assertThat(cmm.get(key1), is(value1));
		value1 = new Value(new byte[]{4,5,6,7});
		cmm.put(key1, value1);
		assertThat(cmm.get(key1), is(value1));

		int key2 = 2;
		Value value2 = new Value(new byte[]{8,9});
		cmm.put(key2, value2);
		assertThat(cmm.get(key2), is(value2));

		int key3 = 3;
		Value value3 = new Value(new byte[]{10,11,12,13});

		// not equal due to space limitation
		cmm.put(key3, value3);
		assertThat(cmm.get(key3), not(value3));
		assertThat(cmm.get(key3), is(Value.NULL));

		// compaction memory buffer to get buffer space.
		cmm.compaction();
		cmm.put(key3, value3);
		assertThat(cmm.get(key3), is(value3));

		// also key1 and key2 are available
		assertThat(cmm.get(key1), is(value1));
		assertThat(cmm.get(key2), is(value2));
	}

	@Test
	public void delete() {
		int key = 1;
		Value value = new Value(new byte[]{1,2,3});
		cmm.put(key, value);
		assertThat(cmm.get(key), is(value));

		assertThat(cmm.delete(key), is(value));
		assertThat(cmm.get(key), is(Value.NULL));
	}

	@Test
	public void LRUReplacement() {
		int key1 = 1;
		Value value1 = new Value(new byte[]{1,2,3});
		int key2 = 2;
		Value value2 = new Value(new byte[]{2,3,4});
		int key3 = 3;
		Value value3 = new Value(new byte[]{3,4,5});
		int key4 = 4;
		Value value4 = new Value(new byte[]{4,5,6});
		int key5 = 5;
		Value value5 = new Value(new byte[]{5,6,7});

		assertThat(cmm.put(key1, value1), is(value1));
		assertThat(cmm.put(key2, value2), is(value2));
		assertThat(cmm.put(key3, value3), is(value3));
		assertThat(cmm.get(key1), is(value1));
		assertThat(cmm.get(key2), is(value2));
		assertThat(cmm.get(key3), is(value3));

		// after insert key4, key1 is replaced due to LRU algorithm.
		assertThat(cmm.put(key4, value4), is(Value.NULL));
		Set<Map.Entry<Integer, Value>> replacies = cmm.replace(key4, value4);
		for(Map.Entry<Integer, Value> replaced : replacies) {
			assertThat(replaced.getKey(), is(key1));
			assertThat(replaced.getValue(), is(value1));
		}
		assertThat(cmm.get(key1), is(Value.NULL));
		assertThat(cmm.get(key2), is(value2));
		assertThat(cmm.get(key3), is(value3));

		// once more replace. after that, the replaced key should be 4.
		assertThat(cmm.put(key5, value5), is(Value.NULL));
		replacies = cmm.replace(key5, value5);
		for(Map.Entry<Integer, Value> replaced : replacies) {
			assertThat(replaced.getKey(), is(key4));
			assertThat(replaced.getValue(), is(value4));
		}
		assertThat(cmm.get(key4), is(Value.NULL));
		assertThat(cmm.get(key2), is(value2));
		assertThat(cmm.get(key3), is(value3));
		assertThat(cmm.get(key5), is(value5));
	}

}
