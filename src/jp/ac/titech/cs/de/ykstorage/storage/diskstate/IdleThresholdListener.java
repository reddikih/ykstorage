package jp.ac.titech.cs.de.ykstorage.storage.diskstate;

public interface IdleThresholdListener {

    public void exceededIdleThreshold(int diskId);
}
