package jp.ac.titech.cs.de.ykstorage.storage.buffer.assignor;

public interface IAssignor {

    public int assign(long blockId, int primaryDiskId, int replicaLevel);

}
