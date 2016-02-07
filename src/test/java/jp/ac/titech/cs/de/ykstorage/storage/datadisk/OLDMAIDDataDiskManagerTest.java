package jp.ac.titech.cs.de.ykstorage.storage.datadisk;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;

import jp.ac.titech.cs.de.ykstorage.storage.datadisk.OLDMAIDDataDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.MAIDDataDiskStateManager;
import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.service.Value;
import jp.ac.titech.cs.de.ykstorage.util.DiskState;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


@RunWith(JUnit4.class)
public class OLDMAIDDataDiskManagerTest {

	private int key = 1;
	private int key2 = 123;
	private int key3 = 123456;
	private Value value = new Value("value".getBytes());
	private Value value2 = new Value("value2".getBytes());
	private Value value3 = new Value("value3".getBytes());
	private OLDMAIDDataDiskManager dm;
	private MAIDDataDiskStateManager ddsm;
//	private String devicePaths[];

	@Before
	public void setUpClass() {
		this.ddsm = new MAIDDataDiskStateManager(Parameter.MOUNT_POINT_PATHS, Parameter.DATA_DISK_PATHS,
				Parameter.SPIN_DOWN_THRESHOLD, Parameter.SPINDOWN_INTERVAL, Parameter.RMI_URL,
				Parameter.IS_CACHEDISK, Parameter.NUMBER_OF_CACHE_DISKS, Parameter.NUMBER_OF_DATA_DISKS,
				Parameter.ACC);
		this.dm = new OLDMAIDDataDiskManager(
				Parameter.DATA_DISK_PATHS,
				Parameter.DATA_DISK_SAVE_FILE_PATH,
				Parameter.MOUNT_POINT_PATHS,
				Parameter.SPIN_DOWN_THRESHOLD,
				ddsm
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

	@Test
	public void loadAndSaveTest() {
		assertThat(dm.put(key, value), is(true));
		assertThat(dm.put(key2, value2), is(true));
		assertThat(dm.put(key3, value3), is(true));
		dm.end();
		
		MAIDDataDiskStateManager ddsm2 = new MAIDDataDiskStateManager(Parameter.MOUNT_POINT_PATHS, Parameter.DATA_DISK_PATHS,
				Parameter.SPIN_DOWN_THRESHOLD, Parameter.SPINDOWN_INTERVAL, Parameter.RMI_URL,
				Parameter.IS_CACHEDISK, Parameter.NUMBER_OF_CACHE_DISKS, Parameter.NUMBER_OF_DATA_DISKS,
				Parameter.ACC);
		
		OLDMAIDDataDiskManager dm2 = new OLDMAIDDataDiskManager(
								Parameter.DATA_DISK_PATHS,
								Parameter.DATA_DISK_SAVE_FILE_PATH,
								Parameter.MOUNT_POINT_PATHS,
								Parameter.SPIN_DOWN_THRESHOLD,
								ddsm2);
		assertThat(dm2.get(key).getValue(), is(value.getValue()));
		assertThat(dm2.get(key2).getValue(), is(value2.getValue()));
		assertThat(dm2.get(key3).getValue(), is(value3.getValue()));
	}

	@Test
	public void getDiskStateTest() {
		assertThat(dm.put(key, value), is(true));
		assertThat(dm.getDiskState(key), is(DiskState.IDLE));

		try {
			Thread.sleep((long) (Parameter.SPIN_DOWN_THRESHOLD * 1000) + 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		assertThat(dm.getDiskState(key), is(DiskState.STANDBY));
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
		File f = new File(Parameter.DATA_DISK_SAVE_FILE_PATH);
		f.delete();
	}

}
