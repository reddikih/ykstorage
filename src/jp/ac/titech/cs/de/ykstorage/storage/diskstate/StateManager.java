package jp.ac.titech.cs.de.ykstorage.storage.diskstate;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StateManager {

    private final static Logger logger = LoggerFactory.getLogger(StateManager.class);

    private DiskState[] diskStates;

    public StateManager(String devicePathPrefix, char[] deviceCharacters) {
        init(devicePathPrefix, deviceCharacters);
    }

    private void init(String devicePathPrefix, char[] deviceCharacters) {
        this.diskStates = new DiskState[deviceCharacters.length];
        for (int i=0; i < deviceCharacters.length; i++) {
            this.diskStates[i] = new DiskState(
                    devicePathPrefix + deviceCharacters[i], DiskStateType.IDLE);
            logger.info("create DiskState. device path: {}", this.diskStates[i].getDevicePath());
        }
    }

}
