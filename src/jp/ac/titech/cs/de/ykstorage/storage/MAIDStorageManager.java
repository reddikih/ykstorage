package jp.ac.titech.cs.de.ykstorage.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.IBufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.ICacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.IDataDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.MAIDDataDiskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MAIDStorageManager extends StorageManager {

    private final static Logger logger = LoggerFactory.getLogger(MAIDStorageManager.class);

    public MAIDStorageManager(
            IBufferManager bufferManager,
            ICacheDiskManager cacheDiskManager,
            IDataDiskManager dataDiskManager,
            Parameter parameter) {
        super(bufferManager, cacheDiskManager, dataDiskManager, parameter);
        watchdogStart();
    }

    public void watchdogStart() {
        //TODO consider refactoring of following code: explicit type casting.
        ((MAIDDataDiskManager)this.dataDiskManager).startWatchDog();
    }

    @Override
    public byte[] read(long key) {
        List<Long> blockIds = getCorrespondingBlockIds(key);
        List<Block> result = new ArrayList<>();
        List<Long> hitMissIds = new ArrayList<>();
        List<Block> tobeCached = new ArrayList<>();

        for (long blockId : blockIds) {
            Block block = this.bufferManager.read(blockId);
            if (block != null)
                result.add(block);
            else
                hitMissIds.add(blockId);
        }

        if (hitMissIds.size() == 0) {
            return convertBlocks2Bytes(result);
        }

        for (long blockId : hitMissIds) {
            Block block = this.cacheDiskManager.read(blockId);
            if (block != null) {
                result.add(block);
                hitMissIds.remove(blockId);
                tobeCached.add(block);
            }
        }

        if (hitMissIds.size() == 0) {
            // to cache the blocks read from cache disks into buffer.
            for (Block block : tobeCached) {
               this.bufferManager.write(block);
            }
            return convertBlocks2Bytes(result);
        }

        List<Block> fromDataDiskBlocks = this.dataDiskManager.read(hitMissIds);

        tobeCached.addAll(fromDataDiskBlocks);
        for (Block block : tobeCached) {
            this.bufferManager.write(block);
            try {
                this.cacheDiskManager.write(block);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        result.addAll(fromDataDiskBlocks);

        return convertBlocks2Bytes(result);
    }

    @Override
    public boolean write(long key, byte[] value) {
        List<Long> blockIds = getCorrespondingBlockIds(key);
        if (blockIds == null || blockIds.size() == 0)
            blockIds = assignBlockIds(key, value.length, Block.BLOCK_SIZE);

        List<Block> blocks = createBlocks(blockIds, value, Block.BLOCK_SIZE);

        // write to buffer
        for (Block block : blocks) {
            this.bufferManager.write(block);
        }

        // write to cache disks as write through policy due to keep reliability of data.
        for (Block block : blocks) {
            try {
                this.cacheDiskManager.write(block);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // write to data disks
        return this.dataDiskManager.write(blocks);
    }

    // TODO pull up method
    private List<Long> assignBlockIds(long key, int byteSize, int blockSize) {
        ArrayList<Long> blockIds = new ArrayList<>();

        int numBlocks = (int)Math.ceil((double)byteSize / blockSize);
        for (int i=0; i<numBlocks; i++) {
            long blockId = this.sequenceNumber.getAndIncrement();
            logger.info("Assigned blockId:{}, key:{}", blockId, key);
            blockIds.add(blockId);
        }
        setRequestKey2BlockIds(key, blockIds);
        return blockIds;
    }

    // TODO pull up method
    private List<Block> createBlocks(List<Long> blockIds, byte[] value, int blockSize) {
        ArrayList<Block> blocks = new ArrayList<>();

        Iterator<Long> ids = blockIds.iterator();
        for (int i=0; i < blockIds.size(); i++) {
            long blockId = ids.next();

            byte[] payload = new byte[blockSize];

            int length = value.length - i * blockSize < blockSize
                    ? value.length - i * blockSize : blockSize;

            System.arraycopy(value, i * blockSize, payload, 0, length);

            Block block = new Block(blockId, 0, assignPrimaryDisk(blockId), 0, payload);

            logger.info("Create block. blockId:{}, length:{} ", blockId, payload.length);

            blocks.add(block);
        }
        return blocks;
    }


    // TODO pull up method
    private byte[] convertBlocks2Bytes(List<Block> blocks) {
        ByteBuffer buff = ByteBuffer.allocate(blocks.size() * Block.BLOCK_SIZE);
        for (Block block : blocks) {
            buff.put(block.getPayload());
        }
        return buff.array();
    }

    // TODO pull up method
    private List<Long> getCorrespondingBlockIds(long key) {
        return this.key2blockIdMap.get(key);
    }

    // TODO pull up method
    private void setRequestKey2BlockIds(long key, List<Long> blockIds) {
        this.key2blockIdMap.put(key, blockIds);
    }

    // TODO pull up method
    private int assignPrimaryDisk(long blockId) {
        return this.dataDiskManager.assignPrimaryDiskId(blockId);
    }

}
