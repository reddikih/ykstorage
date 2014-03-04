package jp.ac.titech.cs.de.ykstorage.storage.buffer;

import jp.ac.titech.cs.de.ykstorage.storage.Block;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class BufferRegion {

    private final static Logger logger = LoggerFactory.getLogger(BufferRegion.class);

    private final int bufferId;

    private final int regionId;

    private int capacity;

    /**
     * Buffered blocks in this buffer region.
     *
     * key: block id
     * value: Block object
     */
    private ConcurrentHashMap<Long, Block> blocksTable = new ConcurrentHashMap<>();

    /**
     * Return a number of buffered blocks for each data disks.
     *
     * key: disk id
     * value: A number of buffered blocks.
     */
    private ConcurrentHashMap<Integer, AtomicInteger> bufferLengthPerDisk = new ConcurrentHashMap<>();


    public BufferRegion(int regionId, int bufferId, int capacity) {
        this.regionId = regionId;
        this.bufferId = bufferId;
        this.capacity = capacity;
    }

    public Block read(long blockId) {
        Block result = this.blocksTable.get(blockId);

        if (result != null) {
            logger.info("Buffer:{} Region:{} READ blockId:{}", bufferId, regionId, result.getBlockId());
        } else {
            logger.info("Buffer:{} Region:{} READ MISS blockId:{}", bufferId, regionId, blockId);
        }

        return result;
    }

    /**
     * Write a block to this buffer region.
     *
     * @param block
     * @return written block object or null when couldn't
     * write due to overflow.
     */
    public Block write(Block block) {
        Block result = blocksTable.get(block.getBlockId());
        if (result != null) {
            result.setPayload(block.getPayload());
            logger.info("Buffer:{} Region:{} UPDATE blockId:{}", bufferId, regionId, block.getBlockId());
        } else {
            if (blocksTable.size() < this.capacity) {
                blocksTable.put(block.getBlockId(), block);

                logger.info("Buffer:{} Region:{} WRITE blockId:{}", bufferId, regionId, block.getBlockId());

                incrementBufferLength(block.getOwnerDiskId());
                result = block;


            } else {
                // overflow
                logger.info("Buffer:{} Region:{} WRITE blockId:{} is overflowed.", bufferId, regionId, block.getBlockId());
                result = null;
            }
        }
        return result;
    }

    public Block remove(long blockId) {
        Block removed = blocksTable.remove(blockId);
        if (removed != null) {
            logger.info("Buffer:{} Region:{} REMOVED blockId:{}", bufferId, regionId, removed.getBlockId());
            decrementBufferLength(removed.getOwnerDiskId());
        }
        return removed;
    }

    private void incrementBufferLength(int diskId) {
        AtomicInteger counter = this.bufferLengthPerDisk.get(diskId);
        if (counter != null) {
            logger.info("Buffer:{} Region:{} incremented length of diskId:{} size:{}", bufferId, regionId, diskId, counter.incrementAndGet());
        } else {
            this.bufferLengthPerDisk.put(diskId, new AtomicInteger(1));
            //log
            logger.info("Buffer:{} Region:{} create buffer length counter of diskId:{} size:1", bufferId, regionId, diskId);
        }
    }

    private void decrementBufferLength(int diskId) {
        AtomicInteger counter = this.bufferLengthPerDisk.get(diskId);
        if (counter != null) {
            int decremented = counter.decrementAndGet();
            logger.info("Buffer:{} Region:{} decremented length of diskId:{} to {}", bufferId, regionId, diskId, decremented);
        } else {
            throw new IllegalStateException(
                    String.format("Decrementing a buffer length counter of the disk that is not exist in this region. Buffer:%d Region:%d DiskId:%d",
                            bufferId, regionId, diskId));
        }
    }

    public int getMaximumBufferLengthDiskId() {
        int maximum = Integer.MIN_VALUE;
        int diskId = -1;
        for (Map.Entry<Integer, AtomicInteger> entry : bufferLengthPerDisk.entrySet()) {
            if (maximum < entry.getValue().get()) {
                maximum = entry.getValue().get();
                diskId = entry.getKey();
            }
        }
        return diskId;
    }

    public List<Block> getBufferedBlocks() {
        return new ArrayList<>(blocksTable.values());
    }
}
