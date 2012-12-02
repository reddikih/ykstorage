package test.jp.ac.titech.cs.de.ykstorage.service.cmm;

import static org.junit.Assert.*;

import java.util.Arrays;

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
		byte[] value1 = {1,2,3};
		assertTrue(cmm.put(key1, value1));
		
		int key2 = 2;
		byte[] value2 = {4,5,6,7,8,9,10};
		assertTrue(cmm.put(key2, value2));

		int key3 = 3;
		byte[] value3 = {11};
		assertFalse(cmm.put(key3, value3));

		assertTrue(Arrays.equals(value1, cmm.get(key1)));
		assertTrue(Arrays.equals(value2, cmm.get(key2)));
		assertTrue(Arrays.equals(value1, cmm.get(key1)));
	}

}
