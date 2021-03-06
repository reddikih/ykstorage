package jp.ac.titech.cs.de.ykstorage.storage.buffer;

import jp.ac.titech.cs.de.ykstorage.storage.Block;
import net.jcip.annotations.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class BufferManager implements IBufferManager {

    private final static Logger logger = LoggerFactory.getLogger(BufferManager.class);

    private long capacity;

    @GuardedBy("this")
    private long size;

    double waterMark;

    private ConcurrentHashMap<Long, Block> hashMap = new ConcurrentHashMap<>();

    private ReplacePolicy<Block> replacePolicy;

    public BufferManager(long capacity, int blockSize, double waterMark) {
        init(capacity, blockSize, waterMark);
    }

    private void init(long capacity, int blockSize, double waterMark) {
        int bufferSize = (int)Math.floor((double)capacity / blockSize);
        this.capacity = bufferSize;
        this.waterMark = waterMark;
        this.replacePolicy = new LRUBuffer<>((int)this.capacity);

        logger.debug(
                "Setup buffer manager. capacity:{}[entries] frameSize:{}[byte], replacePolicy:{}",
                this.capacity, blockSize, this.replacePolicy.getClass().getSimpleName());

        if (this.capacity > Integer.MAX_VALUE)
            logger.info(
                    "Buffer capacity over integer range. It is cast from {}[entries] to {}[entries]",
                    this.capacity, (int)this.capacity);
    }


    @Override
    public Block read(long blockId) {
        Block result = this.hashMap.get(blockId);
        if (result != null) {
            Block replaced = this.replacePolicy.add(result);
            if (replaced != null)
                logger.info("Replace blockId:{} with blockId:{} due to read", result.getBlockId(), blockId);
            else
                logger.info("Read from buffer. blockId:{}", blockId);
            return result;
        }
        logger.info("Read miss at buffer. blockId:{}", blockId);
        return result;
    }

    @Override
    public Block read(Block block) {
        return null;
    }

    @Override
    public Block write(Block block) {
        if (block == null) {
            throw new NullPointerException("To buffer block is null");
        }

        long nowSize;
        synchronized (this) {
            nowSize = this.size;
        }
        if (this.capacity > nowSize) {
            synchronized (this) { this.size++; }

            this.hashMap.put(block.getBlockId(), block);
            Block result = this.replacePolicy.add(block);

            assert result == null : "LRU result is not null!!";

            logger.info("Write to buffer. blockId:{} without replace", block.getBlockId());

            return result;
        } else {
            Block removed = this.replacePolicy.add(block);

            this.hashMap.put(block.getBlockId(), block);

            if (removed == null) {
                logger.debug("Write to buffer blockId:{} due to replaced. But replaced block is null.", block.getBlockId());
                return null;
            }

            logger.info("Write to buffer blockId:{} replaced blockId:{}", block.getBlockId(), removed.getBlockId());

            return this.hashMap.remove(removed.getBlockId());
        }
    }

    @Override
    public Block remove(long blockId) {
        return null;
    }

    @Override
    public Block remove(Block block) {
        return null;
    }
}
