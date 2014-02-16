package jp.ac.titech.cs.de.ykstorage.storage.diskstate;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StateManager {

    private final static Logger logger = LoggerFactory.getLogger(StateManager.class);

    private DiskState[] diskStates;

    private List<IdleThresholdListener> idleThresholdListeners = new ArrayList<>();

    private ScheduledExecutorService idleStateWatchdogTimer;

    public StateManager(String devicePathPrefix, char[] deviceCharacters) {
        init(devicePathPrefix, deviceCharacters);
    }

    private void init(String devicePathPrefix, char[] deviceCharacters) {
        this.diskStates = new DiskState[deviceCharacters.length];
        for (int i=0; i < deviceCharacters.length; i++) {
            this.diskStates[i] = new DiskState(
                    devicePathPrefix + deviceCharacters[i], DiskStateType.IDLE);
            logger.info("create DiskState. diskId: {}, device path: {}", i, this.diskStates[i].getDevicePath());
        }

        this.idleStateWatchdogTimer = Executors.newScheduledThreadPool(deviceCharacters.length);
    }

    public DiskStateType getState(int diskId) {
        return this.diskStates[diskId].getState();
    }

    public void setState(int diskId, DiskStateType spinup) {
        this.diskStates[diskId].setState(spinup);
    }

    public void startIdleStateWatchDog(final int diskId) {
        final Runnable watchdog = new Runnable () {
            @Override
            public void run() {
                // TODO should be implement cancel action.
                for (IdleThresholdListener listener : idleThresholdListeners) {
                    listener.exceededIdleThreshold(diskId);
                }
            }
        };
        idleStateWatchdogTimer.schedule(watchdog, 10, TimeUnit.SECONDS);
    }

    public void resetWatchDogTimer(int diskId) {
    }
}
