package test.jp.ac.titech.cs.de.ykstorage.service.cmm;

import static org.junit.Assert.*;

import java.util.Arrays;

import jp.ac.titech.cs.de.ykstorage.service.Value;
import jp.ac.titech.cs.de.ykstorage.service.cmm.CacheMemoryManager;

import org.junit.Before;
import org.junit.Test;

public class CacheMemoryManagerTest {
	
	private CacheMemoryManager cmm;
	
	@Before
	public void setUpClass() {
		//TODO we can use parameterized tests.
		this.cmm = new CacheMemoryManager(10, 1.0);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void initializeArgumentTest() {
		new CacheMemoryManager(10, 10);
		new CacheMemoryManager(10, -1.0);
		new CacheMemoryManager(10, 1.1);
	}
	
	@Test
	public void testPutAndGet() {
		int key1 = 1;
		Value value1 = new Value(new byte[]{1,2,3});
		assertTrue(cmm.put(key1, value1));
		
		int key2 = 2;
		Value value2 = new Value(new byte[]{4,5,6,7,8,9,10});
		assertTrue(cmm.put(key2, value2));

		int key3 = 3;
		Value value3 = new Value(new byte[]{11});
		assertFalse(cmm.put(key3, value3));

		assertTrue(value1.equals(cmm.get(key1)));
		assertTrue(value2.equals(cmm.get(key2)));
		assertTrue(value1.equals(cmm.get(key1)));
	}

}
