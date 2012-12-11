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

	@Before
	public void setUpClass() {
		this.sm = new StateManager(Parameter.NUMBER_OF_DATADISK, Parameter.SPIN_DOWN_THRESHOLD);
	}

	@Test
	public void startTest() {
		sm.start();
		assertThat(sm.getDiskState(1), is(DiskState.ACTIVE));
	}

	@Test
	public void spinupTest() {
		assertThat(sm.spinup(1), is(true));
	}

	@Test
	public void spindownTest() {
		assertThat(sm.spindown(1), is(true));	// spindown /dev/sdb
	}

	@Test
	public void writeToSpindownDiskTest() {
		int key = 1;
		Value value = new Value("value".getBytes());
		DiskManager dm = new DiskManager(Parameter.DATA_DISK_PATHS, Parameter.DATA_DISK_SAVE_FILE_PATH);

		assertThat(sm.spindown(1), is(true));	// spindown /dev/sdb
		assertThat(sm.spinup(1), is(true));
		assertThat(dm.put(key, value), is(true));
		assertThat(dm.get(key).getValue(), is(value.getValue()));
	}

	@Test
	public void mainTest() {
		assertThat(sm.setDiskState(1, DiskState.ACTIVE), is(true));
		assertThat(sm.setDiskState(2, DiskState.IDLE), is(true));
		assertThat(sm.setDiskState(3, DiskState.STANDBY), is(true));

		assertThat(sm.getDiskState(1), is(DiskState.ACTIVE));
		assertThat(sm.getDiskState(2), is(DiskState.IDLE));
		assertThat(sm.getDiskState(3), is(DiskState.STANDBY));

		assertThat(sm.setIdleIntime(1, System.currentTimeMillis()), is(true));
		assertThat(sm.setIdleIntime(2, System.currentTimeMillis()), is(true));
		assertThat(sm.setIdleIntime(3, System.currentTimeMillis()), is(true));

		assertThat(sm.setDiskState(1, DiskState.IDLE), is(true));
		assertThat(sm.getDiskState(1), is(DiskState.IDLE));
	}
}
