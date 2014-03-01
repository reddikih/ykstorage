package jp.ac.titech.cs.de.ykstorage.storage.buffer;

import jp.ac.titech.cs.de.ykstorage.storage.Block;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.assignor.IAssignor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RAPoSDABufferManager implements IBufferManager {

    private final static Logger logger = LoggerFactory.getLogger(RAPoSDABufferManager.class);

    private HashMap<Integer, List<BufferRegion>> regionTable = new HashMap<>();

    private IAssignor assignor;

    public RAPoSDABufferManager(
            int numberOfBuffers,
            long capacity,
            int blockSize,
            double waterMark,
            int replicaLevel,
            IAssignor assignor) {
        this.assignor = assignor;
        init(numberOfBuffers, capacity, blockSize, replicaLevel);
    }

    private void init(
            int numberOfBuffers,
            long capacity,
            int blockSize,
            int replicaLevel) {

        int regionCapacity = calculateRegionCapacity(
                numberOfBuffers, capacity, blockSize, replicaLevel);

        for (int i=0; i<numberOfBuffers; i++) {
            ArrayList<BufferRegion> regions = new ArrayList<>();
            for (int j=0; j<replicaLevel; j++) {
                regions.add(new BufferRegion(j, i, regionCapacity));
            }
            this.regionTable.put(i, regions);
        }

        logger.debug(
                "Setup buffer manager:{} Buffers:{}, ReplicaLevel:{} Regions:{} RegionCapacity:{}[entries] BlockSize:{}[byte] Assignor:{}",
                this.getClass().getSimpleName(),
                numberOfBuffers,
                replicaLevel,
                numberOfBuffers * replicaLevel,
                regionCapacity,
                blockSize,
                this.assignor.getClass().getSimpleName());
    }

    private int calculateRegionCapacity(
            int numberOfBuffers,
            long capacity,
            int blockSize,
            int replicaLevel) {
        double totalCapacity = Math.ceil((double)capacity / blockSize);
        return (int)Math.ceil(totalCapacity / (numberOfBuffers * replicaLevel));
    }

    private BufferRegion getBufferRegion(int bufferId, int replicaLevel) {
        return this.regionTable.get(bufferId).get(replicaLevel);
    }

    @Override
    @Deprecated
    public Block read(long blockId) {
        throw new IllegalAccessError("This method shouldn't be use in RAPoSDA storage.");
    }

    public Block read(Block block) {

        int bufferId = assignor.assign(
                block.getBlockId(),
                block.getPrimaryDiskId(),
                block.getReplicaLevel());

        BufferRegion region = getBufferRegion(bufferId, block.getReplicaLevel());

        return region.read(block.getBlockId());
    }

    @Override
    public Block write(Block block) {

        int bufferId = assignor.assign(
                block.getBlockId(),
                block.getPrimaryDiskId(),
                block.getReplicaLevel());

        BufferRegion region = getBufferRegion(bufferId, block.getReplicaLevel());

        return region.write(block);
    }

    @Override
    @Deprecated
    public Block remove(long blockId) {
        throw new IllegalAccessError("This method shouldn't be use in RAPoSDA storage.");
    }

    public Block remove(Block block) {

        int bufferId = assignor.assign(
                block.getBlockId(),
                block.getPrimaryDiskId(),
                block.getReplicaLevel());

        BufferRegion region = getBufferRegion(bufferId, block.getReplicaLevel());

        return region.remove(block.getBlockId());
    }

    public int getNumberOfBuffers() {
        return this.regionTable.size();
    }

    public int getNumberOfRegions() {
        int numberOfRegions = 0;
        for (List<BufferRegion> regions : regionTable.values()) {
            numberOfRegions += regions.size();
        }
        return numberOfRegions;
    }

    public int getCorrespondingBufferId(Block block) {
        return assignor.assign(
                block.getBlockId(),
                block.getPrimaryDiskId(),
                block.getReplicaLevel());
    }

    public int getMaximumBufferLengthDiskId(int overflowedBufferId, int replicaLevel) {
        BufferRegion region = getBufferRegion(overflowedBufferId, replicaLevel);
        return region.getMaximumBufferLengthDiskId();
    }

    public List<Block> getBlocksCorrespondingToSpecifiedDisk(int diskId) {
        List<Block> result = new ArrayList<>();
        for (List<BufferRegion> regions : regionTable.values()) {
            for (BufferRegion region : regions) {
                for (Block block : region.getBufferedBlocks()) {
                    if (block.getOwnerDiskId() == diskId)
                        result.add(block);
                }
            }
        }
        return result;
    }

    public List<Block> getBlocksInTheSameRegion(Block block) {

        int bufferId = assignor.assign(
                block.getBlockId(),
                block.getPrimaryDiskId(),
                block.getReplicaLevel());

        BufferRegion region = getBufferRegion(bufferId, block.getReplicaLevel());

        return region.getBufferedBlocks();
    }

    public int getBufferLengthCorrespondingToSpecifiedDisk(int diskId) {
        return getBlocksCorrespondingToSpecifiedDisk(diskId).size();
    }
}
