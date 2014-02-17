package jp.ac.titech.cs.de.ykstorage.storage.diskstate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StateManager {

    private final static Logger logger = LoggerFactory.getLogger(StateManager.class);

    private DiskState[] diskStates;

    private final List<IdleThresholdListener> idleThresholdListeners = new ArrayList<>();

    private ScheduledExecutorService idleStateWatchdogTimer;

    private int idleTimeThreshold;

    private final ConcurrentHashMap<Integer, Future<?>> scheduledTasks = new ConcurrentHashMap<>();

    public StateManager(
            String devicePathPrefix,
            char[] deviceCharacters,
            double idleTimeThreshold) {
        init(devicePathPrefix, deviceCharacters, idleTimeThreshold);
    }

    private void init(
            String devicePathPrefix,
            char[] deviceCharacters,
            double idleTimeThreshold) {
        this.diskStates = new DiskState[deviceCharacters.length];

        this.idleTimeThreshold = (int)(idleTimeThreshold * 1000);

        for (int i=0; i < deviceCharacters.length; i++) {
            this.diskStates[i] = new DiskState(
                    devicePathPrefix + deviceCharacters[i], DiskStateType.IDLE);
            logger.info(
                    "create DiskState. diskId: {}, device path: {}",
                    i, this.diskStates[i].getDevicePath());
        }

        this.idleStateWatchdogTimer =
                Executors.newScheduledThreadPool(deviceCharacters.length);
    }

    public void addListener(IdleThresholdListener listener) {
        this.idleThresholdListeners.add(listener);
    }

    public DiskStateType getState(int diskId) {
        return this.diskStates[diskId].getState();
    }

    public void setState(int diskId, DiskStateType state) {
        this.diskStates[diskId].setState(state);
    }

    public void startIdleStateWatchDog(final int diskId) {
        final Runnable watchdog = new Runnable () {
            @Override
            public void run() {
                for (IdleThresholdListener listener : idleThresholdListeners) {
                    listener.exceededIdleThreshold(diskId);
                }
            }
        };
        final Future<?> scheduledTask =
                idleStateWatchdogTimer.schedule(
                        watchdog, idleTimeThreshold, TimeUnit.SECONDS);

        this.scheduledTasks.putIfAbsent(diskId, scheduledTask);
    }

    public void resetWatchDogTimer(int diskId) {
        Future<?> canceledTask = this.scheduledTasks.remove(diskId);
        if (canceledTask != null) {
            canceledTask.cancel(true);
            logger.debug("reset watchdog diskId: {}", diskId);
        }
    }
}
