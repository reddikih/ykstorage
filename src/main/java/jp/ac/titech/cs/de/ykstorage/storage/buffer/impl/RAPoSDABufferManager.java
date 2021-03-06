package jp.ac.titech.cs.de.ykstorage.storage.buffer.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import jp.ac.titech.cs.de.ykstorage.storage.Block;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.BufferRegion;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.IRAPoSDABufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.assignor.IAssignor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RAPoSDABufferManager implements IRAPoSDABufferManager {

    private final static Logger logger = LoggerFactory.getLogger(RAPoSDABufferManager.class);

    private ConcurrentHashMap<Integer, List<BufferRegion>> regionTable = new ConcurrentHashMap<>();

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
        double totalCapacity = Math.floor((double)capacity / blockSize);
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

    @Override
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

    @Override
    public Block remove(Block block) {

        if (block == null)
            return null;

        int bufferId = assignor.assign(
                block.getBlockId(),
                block.getPrimaryDiskId(),
                block.getReplicaLevel());

        BufferRegion region = getBufferRegion(bufferId, block.getReplicaLevel());

        return region.remove(block.getBlockId());
    }

    @Override
    public int getNumberOfBuffers() {
        return this.regionTable.size();
    }

    @Override
    public int getNumberOfRegions() {
        int numberOfRegions = 0;
        for (List<BufferRegion> regions : regionTable.values()) {
            numberOfRegions += regions.size();
        }
        return numberOfRegions;
    }

    @Override
    public int getCorrespondingBufferId(Block block) {
        return assignor.assign(
                block.getBlockId(),
                block.getPrimaryDiskId(),
                block.getReplicaLevel());
    }

    @Override
    public int getMaximumBufferLengthDiskId(int overflowedBufferId, int replicaLevel) {
        BufferRegion region = getBufferRegion(overflowedBufferId, replicaLevel);
        return region.getMaximumBufferLengthDiskId();
    }

    @Override
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

    @Override
    public List<Block> getBlocksInTheSameRegion(Block block) {

        int bufferId = assignor.assign(
                block.getBlockId(),
                block.getPrimaryDiskId(),
                block.getReplicaLevel());

        BufferRegion region = getBufferRegion(bufferId, block.getReplicaLevel());

        return region.getBufferedBlocks();
    }

    @Override
    public int getBufferLengthCorrespondingToSpecifiedDisk(int diskId) {
        return getBlocksCorrespondingToSpecifiedDisk(diskId).size();
    }
}
