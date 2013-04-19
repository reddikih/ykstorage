package test.jp.ac.titech.cs.de.ykstorage.service;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;

import jp.ac.titech.cs.de.ykstorage.service.DiskManager;
import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.service.Value;
import jp.ac.titech.cs.de.ykstorage.util.DiskState;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;


@RunWith(Enclosed.class)
public class DiskManagerTest {

	static int key = 1;
	static int key2 = 123;
	static int key3 = 123456;
	static Value value = new Value("value".getBytes());
	static Value value2 = new Value("value2".getBytes());
	static Value value3 = new Value("value3".getBytes());
	
	public static class getValueTest {
		DiskManager dm;
		
		@Before
		public void setUp() {
			this.dm = new DiskManager(
					Parameter.DATA_DISK_PATHS,
					Parameter.DATA_DISK_SAVE_FILE_PATH,
					Parameter.MOUNT_POINT_PATHS,
					Parameter.SPIN_DOWN_THRESHOLD);
		}
		
		@Test
		public void getNull() {
			Value actual = dm.get(key);
			assertThat(actual, is(Value.NULL));
		}
		
		@Test
		public void getTest() {
			dm.put(key, value);
			
			Value actual = dm.get(key);
			assertThat(actual, is(value));
		}
		
	}
	
	public static class putValueTest {
		DiskManager dm;
		
		@Before
		public void setUp() {
			this.dm = new DiskManager(
					Parameter.DATA_DISK_PATHS,
					Parameter.DATA_DISK_SAVE_FILE_PATH,
					Parameter.MOUNT_POINT_PATHS,
					Parameter.SPIN_DOWN_THRESHOLD);
		}
		
		@Test
		public void putTest() {
			boolean actual = dm.put(key, value);
			assertThat(actual, is(true));
		}
		
		@Test
		public void putValuesTest() {
			dm.put(key, value);
			dm.put(key2, value2);
			
			Value actual = dm.get(key);
			assertThat(actual, is(value));
		}
		
		@Test
		public void updateTest() {
			dm.put(key, value);
			dm.put(key, value2);
			
			Value actual = dm.get(key);
			assertThat(actual, is(value2));
		}
		
		@Test
		public void putAfterDeleteTest() {
			dm.put(key, value);
			dm.delete(key);
			dm.put(key, value2);
			
			Value actual = dm.get(key);
			assertThat(actual, is(value2));
		}
	}
	
	public static class deleteValueTest {
		DiskManager dm;
		
		@Before
		public void setUp() {
			this.dm = new DiskManager(
					Parameter.DATA_DISK_PATHS,
					Parameter.DATA_DISK_SAVE_FILE_PATH,
					Parameter.MOUNT_POINT_PATHS,
					Parameter.SPIN_DOWN_THRESHOLD);
		}
		
		@Test
		public void deleteNull() {
			boolean actual = dm.delete(key);
			assertThat(actual, is(false));
		}
		
		@Test
		public void deleteTest() {
			dm.put(key, value);
			
			boolean actual = dm.delete(key);
			assertThat(actual, is(true));
		}
		
	}
	
	public static class loadAndSaveTest {
		DiskManager dm;
		
		@Before
		public void setUp() {
			this.dm = new DiskManager(
					Parameter.DATA_DISK_PATHS,
					Parameter.DATA_DISK_SAVE_FILE_PATH,
					Parameter.MOUNT_POINT_PATHS,
					Parameter.SPIN_DOWN_THRESHOLD);
			
			dm.put(key, value);
			dm.put(key2, value2);
			dm.put(key3, value3);
		}
		
		@Test
		public void saveTest() {
			dm.end();
		}
		
		@Test
		public void loadTest() {
			dm.end();
			DiskManager dm2 = new DiskManager(
					Parameter.DATA_DISK_PATHS,
					Parameter.DATA_DISK_SAVE_FILE_PATH,
					Parameter.MOUNT_POINT_PATHS,
					Parameter.SPIN_DOWN_THRESHOLD);
			
			Value actual = dm2.get(key);
			assertThat(actual, is(value));
		}
		
		@After
		public void teardown() {
			File f = new File(Parameter.DATA_DISK_SAVE_FILE_PATH);
			f.delete();
		}
	}
	
	public static class getDiskStateTest {
		DiskManager dm;
		
		@Before
		public void setUp() {
			this.dm = new DiskManager(
					Parameter.DATA_DISK_PATHS,
					Parameter.DATA_DISK_SAVE_FILE_PATH,
					Parameter.MOUNT_POINT_PATHS,
					Parameter.SPIN_DOWN_THRESHOLD);
		}
		
		@Test
		public void getNullStateTest() {
			DiskState actual = dm.getDiskState(key);
			assertThat(actual, is(DiskState.NA));
		}
		
		@Test
		public void getStateAfterPutTest() {
			dm.put(key, value);
			
			DiskState actual = dm.getDiskState(key);
			assertThat(actual, is(DiskState.IDLE));
		}
		
		@Test
		public void getStateAfterSpindown() {
			Assume.assumeTrue(System.getProperty("os.name").contains("Linux"));
			
			dm.put(key, value);
			assertThat(dm.getDiskState(key), is(DiskState.IDLE));
			
			try {
				Thread.sleep((long) (Parameter.SPIN_DOWN_THRESHOLD * 1000) + 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			DiskState actual = dm.getDiskState(key);
			assertThat(actual, is(DiskState.STANDBY));
		}
		
	}
	
}
