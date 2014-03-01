package jp.ac.titech.cs.de.ykstorage.storage.datadisk;

import jp.ac.titech.cs.de.ykstorage.storage.Block;
import jp.ac.titech.cs.de.ykstorage.storage.diskstate.DiskStateType;

import java.util.List;

public class RAPoSDADataDiskManager implements IDataDiskManager {

    @Override
    @Deprecated
    public List<Block> read(List<Long> blockIds) {
        return null;
    }

    public Block read(Block block) {
        return null;
    }

    @Override
    @Deprecated
    public boolean write(List<Block> blocks) {
        throw new IllegalAccessError("This method shouldn't be use in RAPoSDA storage.");
    }

    /**
     *
     * @param blocks
     * @return A block list that includes blocks written to data disks.
     */
    public List<Block> writeBlocks(List<Block> blocks) {
        return null;
    }

    @Override
    public int assignPrimaryDiskId(long blockId) {
        return 0;
    }

    public void spinUpDiskIfSleeping(int diskId) {

    }

    public DiskStateType getState(int diskId) {
        return null;
    }

    public long getSleepingTimeByDiskId(int diskId) {
        return 0;
    }

    public int assignReplicaDiskId(int primaryDiskId, int replicaLevel) {
        return 0;
    }
}
