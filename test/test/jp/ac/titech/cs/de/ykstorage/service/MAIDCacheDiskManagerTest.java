package test.jp.ac.titech.cs.de.ykstorage.service;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;

import jp.ac.titech.cs.de.ykstorage.service.MAIDCacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.service.Value;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


@RunWith(JUnit4.class)
public class MAIDCacheDiskManagerTest {
	private static final long CAPACITY_OF_CACHEDISK = 30;

	private int key = 1;
	private int key2 = 123;
	private int key3 = 123456;
	private Value value = new Value("value".getBytes());
	private Value value2 = new Value("value2".getBytes());
	private Value value3 = new Value("value3".getBytes());
	private MAIDCacheDiskManager dm;
//	private String devicePaths[];

	@Before
	public void setUpClass() {
		this.dm = new MAIDCacheDiskManager(
				Parameter.DATA_DISK_PATHS,
				Parameter.DATA_DISK_SAVE_FILE_PATH,
				Parameter.MOUNT_POINT_PATHS,
				Parameter.SPIN_DOWN_THRESHOLD,
				CAPACITY_OF_CACHEDISK
		);
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

//	@Test
//	public void loadAndSaveTest() {
//		assertThat(dm.put(key, value), is(true));
//		assertThat(dm.put(key2, value2), is(true));
//		assertThat(dm.put(key3, value3), is(true));
//		dm.end();
//		MAIDCacheDiskManager dm2 = new MAIDCacheDiskManager(
//								Parameter.DATA_DISK_PATHS,
//								Parameter.DATA_DISK_SAVE_FILE_PATH,
//								Parameter.MOUNT_POINT_PATHS,
//								Parameter.SPIN_DOWN_THRESHOLD);
//		assertThat(dm2.get(key).getValue(), is(value.getValue()));
//		assertThat(dm2.get(key2).getValue(), is(value2.getValue()));
//		assertThat(dm2.get(key3).getValue(), is(value3.getValue()));
//	}

//	@Test
//	public void getDiskStateTest() {
//		assertThat(dm.getDiskState(devicePaths[0]), is(DiskState.IDLE));
//		assertThat(dm.put(key, value), is(true));
//		assertThat(dm.getDiskState(devicePaths[1]), is(DiskState.IDLE));
//
//		try {
//			Thread.sleep((long) (Parameter.SPIN_DOWN_THRESHOLD * 1000));
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//
//		assertThat(dm.getDiskState(devicePaths[0]), is(DiskState.STANDBY));
//	}

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
	
	@Test
	public void LRUPutTest() {
		Value v = new Value("valuevalue".getBytes());
		int key4 = 4;
		assertThat(dm.put(key, v), is(true));
		assertThat(dm.put(key2, v), is(true));
		assertThat(dm.put(key3, v), is(true));
		assertThat(dm.put(key4, v), is(true));
	}
	
	@Test
	public void LRUGetTest() {
		Value v = new Value("valuevalue".getBytes());
		int key4 = 4;
		assertThat(dm.put(key, v), is(true));
		assertThat(dm.put(key2, v), is(true));
		assertThat(dm.put(key3, v), is(true));
		assertThat(dm.put(key4, v), is(true));
		assertThat(dm.get(key), is(Value.NULL));
		assertThat(dm.get(key2).getValue(), is(v.getValue()));
	}
	
	@Test
	public void LRUUpdateInfoTest() {
		Value v = new Value("valuevalue".getBytes());
		int key4 = 4;
		assertThat(dm.put(key, v), is(true));
		assertThat(dm.put(key2, v), is(true));
		assertThat(dm.put(key3, v), is(true));
		assertThat(dm.get(key).getValue(), is(v.getValue()));
		assertThat(dm.put(key4, v), is(true));
		assertThat(dm.get(key).getValue(), is(v.getValue()));
		assertThat(dm.get(key2), is(Value.NULL));
	}
	
	@Test
	public void LRUPutLargeSizeTest() {
		Value v = new Value("value value value value value value value".getBytes());
		assertThat(dm.put(key, v), is(false));
	}

	@After
	public void teardown() {
		for(String path : Parameter.DATA_DISK_PATHS) {
			File f = new File(path);
			f.delete();
		}
		File f = new File(Parameter.DATA_DISK_SAVE_FILE_PATH);
		f.delete();
	}

}
