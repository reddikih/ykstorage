package test.jp.ac.titech.cs.de.ykstorage.service;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.service.StateManager;


@RunWith(JUnit4.class)
public class StateManagerTest {
	
	@Test
	public void startTest() {
		StateManager sm = new StateManager(Parameter.NUMBER_OF_DATADISK, Parameter.SPIN_DOWN_THRESHOLD);
		sm.start();
		assertThat(sm.getDiskState(0), is(StateManager.ACTIVE));
	}
	
	@Test
	public void spindownTest() {
		StateManager sm = new StateManager(Parameter.NUMBER_OF_DATADISK, Parameter.SPIN_DOWN_THRESHOLD);
		assertThat(sm.spindown(1), is(true));	// spindown /dev/sdb
	}
	
	@Test
	public void mainTest() {
		StateManager sm = new StateManager(Parameter.NUMBER_OF_DATADISK, Parameter.SPIN_DOWN_THRESHOLD);
		
		assertThat(sm.setDiskState(0, StateManager.ACTIVE), is(true));
		assertThat(sm.setDiskState(1, StateManager.IDLE), is(true));
		assertThat(sm.setDiskState(2, StateManager.STANDBY), is(true));
		
		assertThat(sm.getDiskState(0), is(StateManager.ACTIVE));
		assertThat(sm.getDiskState(1), is(StateManager.IDLE));
		assertThat(sm.getDiskState(2), is(StateManager.STANDBY));
		
		assertThat(sm.setIdleIntime(0, System.currentTimeMillis()), is(true));
		assertThat(sm.setIdleIntime(1, System.currentTimeMillis()), is(true));
		assertThat(sm.setIdleIntime(2, System.currentTimeMillis()), is(true));
		
		assertThat(sm.setDiskState(0, StateManager.IDLE), is(true));
		assertThat(sm.getDiskState(0), is(StateManager.IDLE));
	}
}
