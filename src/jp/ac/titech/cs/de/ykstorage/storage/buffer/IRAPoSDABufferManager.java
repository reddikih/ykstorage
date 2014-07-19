package jp.ac.titech.cs.de.ykstorage.storage.buffer;

import java.util.List;
import jp.ac.titech.cs.de.ykstorage.storage.Block;

public interface IRAPoSDABufferManager extends IBufferManager {

    public int getNumberOfBuffers();

    public int getNumberOfRegions();

    public int getCorrespondingBufferId(Block block);

    public int getMaximumBufferLengthDiskId(int overflowedBufferId, int replicaLevel);

    public List<Block> getBlocksCorrespondingToSpecifiedDisk(int diskId);

    public List<Block> getBlocksInTheSameRegion(Block block);

    public int getBufferLengthCorrespondingToSpecifiedDisk(int diskId);
}
