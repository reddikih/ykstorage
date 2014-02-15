package jp.ac.titech.cs.de.ykstorage.storage.diskstate;

public class DiskState {

    private final String devicePath;
    private DiskStateType state;

    public DiskState(String devicePath, DiskStateType state) {
        this.devicePath = devicePath;
        this.state = state;
    }

    public String getDevicePath() {return this.devicePath;}

    public DiskStateType getState() {return this.state;}

    public void setState(DiskStateType state) {this.state = state;}

}
