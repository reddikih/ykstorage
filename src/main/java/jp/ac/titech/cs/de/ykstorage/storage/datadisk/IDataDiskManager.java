package jp.ac.titech.cs.de.ykstorage.storage.datadisk;

import jp.ac.titech.cs.de.ykstorage.storage.Block;

import java.util.List;

public interface IDataDiskManager {

    public List<Block> read(List<Long> blockIds);

    public boolean write(List<Block> blocks);

    public int assignPrimaryDiskId(long blockId);

    public int assignReplicaDiskId(int primaryDiskId, int replicaLevel);

    public void setDeleteOnExit(boolean deleteOnExit);

    public void startWatchDog();

    public void termination();
}
