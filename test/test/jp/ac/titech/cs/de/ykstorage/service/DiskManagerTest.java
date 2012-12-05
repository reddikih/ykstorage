package test.jp.ac.titech.cs.de.ykstorage.service;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import jp.ac.titech.cs.de.ykstorage.service.DiskManager;
import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.service.Value;


@RunWith(JUnit4.class)
public class DiskManagerTest {
	
	private int key = 1;
	private int key2 = 123;
	private int key3 = 123456;
	private Value value = new Value("value".getBytes());
	private Value value2 = new Value("value2".getBytes());
	private Value value3 = new Value("value3".getBytes());
	private DiskManager dm;
	
	@Before
	public void setUpClass() {
		this.dm = new DiskManager(Parameter.DATA_DISK_PATHS);
	}
	
	@Test(expected=SecurityException.class)
	public void initMkdirsTest() {
		String[] diskpath = {"!?"}; 
		new DiskManager(diskpath);
	}
	
	@Test
	public void testGet() {
		assertThat(dm.get(key), is(Value.NULL));
	}

	@Test
	public void testPut() {
		assertThat(dm.put(key, value), is(true));
	}
	
	@Test
	public void testDelete() {
		assertThat(dm.delete(key), is(false));
	}
	
	@Test
	public void mainTest() {
		assertThat(dm.put(key, value), is(true));
		assertThat(dm.get(key).getValue(), is(value.getValue()));
		assertThat(dm.delete(key), is(true));
	}
	
	@Test
	public void mainTest2() {
		assertThat(dm.put(key, value), is(true));
		assertThat(dm.put(key2, value2), is(true));
		assertThat(dm.get(key).getValue(), is(value.getValue()));
		assertThat(dm.get(key2).getValue(), is(value2.getValue()));
		
		assertThat(dm.put(key, value2), is(true));
		assertThat(dm.get(key).getValue(), is(value2.getValue()));
		assertThat(dm.put(key2, value), is(true));
		assertThat(dm.get(key2).getValue(), is(value.getValue()));
		
		assertThat(dm.put(key3, value3), is(true));
		assertThat(dm.get(key3).getValue(), is(value3.getValue()));
		
		assertThat(dm.delete(key), is(true));
		assertThat(dm.delete(key2), is(true));
		assertThat(dm.delete(key3), is(true));
		
		assertThat(dm.get(key), is(Value.NULL));
		assertThat(dm.put(key, value), is(true));
		assertThat(dm.get(key).getValue(), is(value.getValue()));
		assertThat(dm.delete(key), is(true));
	}
	
	@After
	public void teardown() {
		for(String path : Parameter.DATA_DISK_PATHS) {
			File f = new File(path);
			f.delete();
		}
	}

}
