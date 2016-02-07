package jp.ac.titech.cs.de.ykstorage.storage.diskstate;

import net.jcip.annotations.GuardedBy;

public class DiskState {

    private final String devicePath;

    @GuardedBy("this")
    private DiskStateType state;

    @GuardedBy("this")
    private long standbyStartTime;

    public DiskState(String devicePath, DiskStateType state) {
        this.devicePath = devicePath;
        this.state = state;
    }

    public String getDevicePath() {
        return this.devicePath;
    }

    public synchronized DiskStateType getState() {
        return this.state;
    }

    public synchronized void setState(DiskStateType state) {
        this.state = state;
        if (state.equals(DiskStateType.STANDBY)) {
            this.standbyStartTime = System.nanoTime();
        }
    }

    public synchronized long getStandbyStartTime() {
        if (this.state.equals(DiskStateType.STANDBY)) {
            return this.standbyStartTime;
        } else {
            return Long.MAX_VALUE;
        }
    }

}
