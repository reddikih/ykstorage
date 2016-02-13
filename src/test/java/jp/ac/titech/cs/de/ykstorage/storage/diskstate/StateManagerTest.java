package test.jp.ac.titech.cs.de.ykstorage.storage.diskstate;

import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.diskstate.DiskStateType;
import jp.ac.titech.cs.de.ykstorage.storage.diskstate.StateManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@RunWith(JUnit4.class)
public class StateManagerTest {

    private String devicePathPrefix = "/dev/sd";
    private String configPath = "./config/config.properties";
    private String[] deviceCharacters = new Parameter(configPath).driveCharacters;

    @Test
    public void getAStateFromStateManager() {
        StateManager stm = new StateManager(devicePathPrefix, deviceCharacters, 10.0);

        DiskStateType state = stm.getState(0);
        assertThat(state, is(DiskStateType.IDLE));
    }



}
