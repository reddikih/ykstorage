package jp.ac.titech.cs.de.ykstorage.storage.cachedisk;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.MAIDCacheDiskStateManager;
import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.util.DiskState;

@Ignore
public class MAIDCacheDiskStateManagerTest {
	private MAIDCacheDiskStateManager sm;
	private String devicePaths[];

	@Before
	public void setUpClass() {
		this.sm = new MAIDCacheDiskStateManager(Parameter.MOUNT_POINT_PATHS, Parameter.CACHE_DISK_PATHS,
				Parameter.ACCESS_THRESHOLD, Parameter.ACCESS_INTERVAL, Parameter.RMI_URL,
				Parameter.IS_CACHEDISK, Parameter.NUMBER_OF_CACHE_DISKS, Parameter.NUMBER_OF_DATA_DISKS);
		
		this.devicePaths = new String[Parameter.NUMBER_OF_CACHE_DISKS];
		for (int i=0; i < devicePaths.length; i++) {
			devicePaths[i] = Parameter.MOUNT_POINT_PATHS.get(Parameter.CACHE_DISK_PATHS[i]);
		}
	}

	@Test
	public void startTest() {
		sm.start();
		assertThat(sm.getDiskState(devicePaths[0]), is(DiskState.IDLE));
		try {
			Thread.sleep(Parameter.ACCESS_INTERVAL + 1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void spinupTest() {
		assertThat(sm.spinup(devicePaths[0]), is(true));
	}

	@Test
	public void spindownTest() {
		assertThat(sm.spindown(devicePaths[0]), is(true));	// spindown /dev/sdb
	}

//	@Test
//	public void writeToSpindownDiskTest() {
//		int key = 1;
//		Value value = new Value("value".getBytes());
//		DiskManager dm = new DiskManager(
//				Parameter.DATA_DISK_PATHS,
//				Parameter.DATA_DISK_SAVE_FILE_PATH,
//				Parameter.MOUNT_POINT_PATHS,
//				Parameter.SPIN_DOWN_THRESHOLD);
//
//		assertThat(sm.spindown(devicePaths[0]), is(true));	// spindown /dev/sdb
//		assertThat(sm.spinup(devicePaths[0]), is(true));
//		assertThat(dm.put(key, value), is(true));
//		assertThat(dm.get(key).getValue(), is(value.getValue()));
//	}

//	@Test
//	public void mainTest() {
//		assertThat(sm.setDiskState(devicePaths[0], DiskState.ACTIVE), is(true));
//		assertThat(sm.setDiskState(devicePaths[1], DiskState.IDLE), is(true));
//		assertThat(sm.setDiskState(devicePaths[2], DiskState.STANDBY), is(true));
//
//		assertThat(sm.getDiskState(devicePaths[0]), is(DiskState.ACTIVE));
//		assertThat(sm.getDiskState(devicePaths[1]), is(DiskState.IDLE));
//		assertThat(sm.getDiskState(devicePaths[2]), is(DiskState.STANDBY));
//
//		assertThat(sm.setIdleIntime(devicePaths[0], System.currentTimeMillis()), is(true));
//		assertThat(sm.setIdleIntime(devicePaths[1], System.currentTimeMillis()), is(true));
//		assertThat(sm.setIdleIntime(devicePaths[2], System.currentTimeMillis()), is(true));
//
//		assertThat(sm.setDiskState(devicePaths[0], DiskState.IDLE), is(true));
//		assertThat(sm.getDiskState(devicePaths[0]), is(DiskState.IDLE));
//	}

	@After
	public void teardown() {
		for(String path : Parameter.CACHE_DISK_PATHS) {
			File f = new File(path);
			f.delete();
		}
		File f = new File(Parameter.DATA_DISK_SAVE_FILE_PATH);
		f.delete();
	}
}
