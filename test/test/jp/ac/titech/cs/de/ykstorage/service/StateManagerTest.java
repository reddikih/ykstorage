package test.jp.ac.titech.cs.de.ykstorage.service;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import jp.ac.titech.cs.de.ykstorage.service.DiskManager;
import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.service.StateManager;
import jp.ac.titech.cs.de.ykstorage.service.Value;
import jp.ac.titech.cs.de.ykstorage.util.DiskState;


@RunWith(JUnit4.class)
public class StateManagerTest {
	private StateManager sm;
	private String devicePaths[];

	@Before
	public void setUpClass() {
		this.sm = new StateManager(Parameter.MOUNT_POINT_PATHS.values(), Parameter.SPIN_DOWN_THRESHOLD);
		this.devicePaths = new String[Parameter.DATA_DISK_PATHS.length];
		for (int i=0; i < devicePaths.length; i++) {
			devicePaths[i] = Parameter.MOUNT_POINT_PATHS.get(Parameter.DATA_DISK_PATHS[i]);
		}
	}

	@Test
	public void startTest() {
		sm.start();
		assertThat(sm.getDiskState(devicePaths[0]), is(DiskState.IDLE));
	}

	@Test
	public void spinupTest() {
		assertThat(sm.spinup(devicePaths[0]), is(true));
	}

	@Test
	public void spindownTest() {
		assertThat(sm.spindown(devicePaths[0]), is(true));	// spindown /dev/sdb
	}

	@Test
	public void writeToSpindownDiskTest() {
		int key = 1;
		Value value = new Value("value".getBytes());
		DiskManager dm = new DiskManager(
				Parameter.DATA_DISK_PATHS,
				Parameter.DATA_DISK_SAVE_FILE_PATH,
				Parameter.MOUNT_POINT_PATHS,
				Parameter.SPIN_DOWN_THRESHOLD);

		assertThat(sm.spindown(devicePaths[0]), is(true));	// spindown /dev/sdb
		assertThat(sm.spinup(devicePaths[0]), is(true));
		assertThat(dm.put(key, value), is(true));
		assertThat(dm.get(key).getValue(), is(value.getValue()));
	}

	@Test
	public void mainTest() {
		assertThat(sm.setDiskState(devicePaths[0], DiskState.ACTIVE), is(true));
		assertThat(sm.setDiskState(devicePaths[1], DiskState.IDLE), is(true));
		assertThat(sm.setDiskState(devicePaths[2], DiskState.STANDBY), is(true));

		assertThat(sm.getDiskState(devicePaths[0]), is(DiskState.ACTIVE));
		assertThat(sm.getDiskState(devicePaths[1]), is(DiskState.IDLE));
		assertThat(sm.getDiskState(devicePaths[2]), is(DiskState.STANDBY));

		assertThat(sm.setIdleIntime(devicePaths[0], System.currentTimeMillis()), is(true));
		assertThat(sm.setIdleIntime(devicePaths[1], System.currentTimeMillis()), is(true));
		assertThat(sm.setIdleIntime(devicePaths[2], System.currentTimeMillis()), is(true));

		assertThat(sm.setDiskState(devicePaths[0], DiskState.IDLE), is(true));
		assertThat(sm.getDiskState(devicePaths[0]), is(DiskState.IDLE));
	}
}
