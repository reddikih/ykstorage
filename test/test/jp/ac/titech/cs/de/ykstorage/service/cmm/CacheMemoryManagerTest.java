package test.jp.ac.titech.cs.de.ykstorage.service.cmm;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
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
		assertThat(cmm.put(key3, value3), is(false));

		assertThat(cmm.get(key1), is(value1));
		assertThat(cmm.get(key2), is(value2));
		assertThat(cmm.get(key1), is(value1));
		assertThat(cmm.get(key1), not(value2));
	}

	@Test
	public void updateTheSameKeys() {
		int key1 = 1;
		Value value1 = new Value(new byte[]{1,2,3});
		Value value2 = new Value(new byte[]{4,5,6,7});
		cmm.put(key1, value1);
		assertThat(cmm.get(key1), is(value1));
		cmm.put(key1, value2);
		assertThat(cmm.get(key1), is(value2));

		int key2 = 2;
		Value value3 = new Value(new byte[]{8,9,10,11,12,13});
		cmm.put(key2, value3);
		assertThat(cmm.get(key2), is(value3));
	}

	@Test
	public void getNullObjectByInvalidKey() {
		int key = 1;
		Value value = new Value(new byte[]{1,2,3});
		cmm.put(key, value);
		assertThat(cmm.get(key), is(value));

		int invalidKey = 2;// invalid key id
		assertThat(cmm.get(invalidKey), is(Value.NULL));
	}

	@Test
	public void idempotentPut() {
		int key = 1;
		Value value = new Value(new byte[]{1,2,3});
		cmm.put(key, value);
		assertThat(cmm.get(key), is(value));
		assertThat(cmm.get(key), is(value));
		assertThat(cmm.get(key), is(value));
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

}
