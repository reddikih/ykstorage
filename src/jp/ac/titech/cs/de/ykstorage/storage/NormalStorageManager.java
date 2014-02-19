package jp.ac.titech.cs.de.ykstorage.storage;

import java.util.Iterator;
import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.IBufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.ICacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.IDataDiskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class NormalStorageManager extends StorageManager {

    private final static Logger logger = LoggerFactory.getLogger(NormalStorageManager.class);

    public NormalStorageManager(
            IBufferManager bufferManager,
            ICacheDiskManager cacheDiskManager,
            IDataDiskManager dataDiskManager,
            Parameter parameter) {
        super(bufferManager, cacheDiskManager, dataDiskManager, parameter);
        this.key2blockIdMap = new ConcurrentHashMap<>();
    }

    @Override
    public byte[] read(long key) {
        List<Long> blockIds = getCorrespondingBlockIds(key);
        List<Block> result = new ArrayList<>();
        for (Long blockId : blockIds) {
            Block block = this.bufferManager.read(blockId);
            if (block != null)
                result.add(block);
        }

        if (result.size() > 0) {
            return convertBlocks2Bytes(result);
        }

        List<Long> requestBlocks = getCorrespondingBlockIds(key);
        result = this.dataDiskManager.read(requestBlocks);

        return convertBlocks2Bytes(result);
    }

    @Override
    public boolean write(long key, byte[] value) {
        List<Long> blockIds = getCorrespondingBlockIds(key);
        if (blockIds == null || blockIds.size() == 0) {
            blockIds = assignBlockIds(key, value.length, Block.BLOCK_SIZE);
        }
        List<Block> blocks = createBlocks(blockIds, value, Block.BLOCK_SIZE);
        return this.dataDiskManager.write(blocks);
    }

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

    private List<Block> createBlocks(List<Long> blockIds, byte[] value, int blockSize) {
        ArrayList<Block> blocks = new ArrayList<>();

        Iterator<Long> ids = blockIds.iterator();
        for (int i=0; i < blockIds.size(); i++) {
            long blockId = ids.next();

            byte[] payload = new byte[blockSize];

            int length = value.length - i * blockSize < blockSize
                    ? value.length - i * blockSize : blockSize;

            System.arraycopy(value, i * blockSize, payload, 0, length);

            Block block = new Block(blockId, 0, assginPrimaryDisk(blockId), 0, payload);

            logger.info("Create block. blockId:{}, length:{} ", blockId, payload.length);

            blocks.add(block);
        }
        return blocks;
    }

    private byte[] convertBlocks2Bytes(List<Block> blocks) {
        ByteBuffer buff = ByteBuffer.allocate(blocks.size() * Block.BLOCK_SIZE);
        for (Block block : blocks) {
            buff.put(block.getPayload());
        }
        return buff.array();
    }

    private List<Long> getCorrespondingBlockIds(long key) {
        return this.key2blockIdMap.get(key);
    }

    private void setRequestKey2BlockIds(long key, List<Long> blockIds) {
        this.key2blockIdMap.put(key, blockIds);
    }

    private int assginPrimaryDisk(long blockId) {
        return this.dataDiskManager.assginPrimaryDiskId(blockId);
    }

}
