package test.jp.ac.titech.cs.de.ykstorage.service;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import jp.ac.titech.cs.de.ykstorage.service.DiskManager;


@RunWith(JUnit4.class)
public class DiskManagerTest {
	
	private String key = "key";
	private String key2 = "key2";
	private String key3 = "key3";
	private String nullKey = null;
	private String emptyKey = "";
	private byte[] value = "value".getBytes();
	private byte[] value2 = "value2".getBytes();
	private byte[] value3 = "value3".getBytes();
	private byte[] value4 = new byte[1024 * 1024 * 10];
	private DiskManager dm = new DiskManager();
	
	
	@Test
	public void testGet() {
		assertThat(dm.get(key), is(nullValue()));
		assertThat(dm.get(nullKey), is(nullValue()));
		assertThat(dm.get(emptyKey), is(nullValue()));
		//fail("Not yet implemented");
	}

	@Test
	public void testPut() {
		assertThat(dm.put(key, value), is(true));
		assertThat(dm.put(nullKey, value), is(false));
		assertThat(dm.put(emptyKey, value), is(false));
		//fail("Not yet implemented");
	}
	
	@Test
	public void testDelete() {
		assertThat(dm.delete(key), is(false));
		assertThat(dm.delete(nullKey), is(false));
		assertThat(dm.delete(emptyKey), is(false));
		//fail("Not yet implemented");
	}
	
	@Test
	public void mainTest() {
		DiskManager diskMgr = new DiskManager();
		assertThat(diskMgr.put(key, value), is(true));
		assertThat(diskMgr.get(key), is(value));
		assertThat(diskMgr.delete(key), is(true));
	}
	
	@Test
	public void mainTest2() {
		DiskManager diskMgr = new DiskManager();
		assertThat(diskMgr.put(key, value), is(true));
		assertThat(diskMgr.put(key2, value2), is(true));
		assertThat(diskMgr.get(key), is(value));
		assertThat(diskMgr.get(key2), is(value2));
		
		assertThat(diskMgr.put(key, value2), is(true));
		assertThat(diskMgr.get(key), is(value2));
		assertThat(diskMgr.put(key2, value), is(true));
		assertThat(diskMgr.get(key2), is(value));
		
		assertThat(diskMgr.put(key3, value3), is(true));
		assertThat(diskMgr.get(key3), is(value3));
		
		assertThat(diskMgr.delete(key), is(true));
		assertThat(diskMgr.delete(key2), is(true));
		assertThat(diskMgr.delete(key3), is(true));
		
		assertThat(diskMgr.get(key), is(nullValue()));
		assertThat(diskMgr.put(key, value), is(true));
		assertThat(diskMgr.get(key), is(value));
		assertThat(diskMgr.delete(key), is(true));
	}
	
	@Test
	public void mainTest3() {
		for(int i = 0; i < value4.length; i++) {
			value4[i] = (byte) i;
		}
		DiskManager diskMgr = new DiskManager();
		assertThat(diskMgr.put(key, value4), is(true));
		assertThat(diskMgr.get(key), is(value4));
		//assertThat(diskMgr.delete(key), is(true));
	}

}
