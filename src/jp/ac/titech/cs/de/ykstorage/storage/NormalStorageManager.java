package jp.ac.titech.cs.de.ykstorage.storage;

import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.IBufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.ICacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.IDataDiskManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class NormalStorageManager extends StorageManager {

    public NormalStorageManager(
            IBufferManager bufferManager,
            ICacheDiskManager cacheDiskManager,
            IDataDiskManager dataDiskManager,
            Parameter parameter) {
        super(bufferManager, cacheDiskManager, dataDiskManager, parameter);
        this.key2blockIdMap = new ConcurrentHashMap<Long, List<Long>>();
    }

    @Override
    public byte[] read(long key) {
        List<Long> blockIds = getCorrespondingBlockIds(key);
        List<Block> result = new ArrayList<Block>();
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
        List<Block> blocks = assignBlock(key, value);
        return this.dataDiskManager.write(blocks);
    }

    private List<Block> assignBlock(long key, byte[] value) {
        ArrayList<Block> blocks = new ArrayList<Block>();
        ArrayList<Long> blockIds = new ArrayList<Long>();

        int numBlocks = (int)Math.ceil((double)value.length / Block.BLOCK_SIZE);
        for (int i=0; i<numBlocks; i++) {
            long blockId = this.sequenceNumber.getAndIncrement();

            byte[] payload = new byte[Block.BLOCK_SIZE];
            System.arraycopy(value, i * Block.BLOCK_SIZE, payload, 0, Block.BLOCK_SIZE);
            Block block = new Block(blockId, 0, assginPrimaryDisk(blockId), 0, payload);

            blockIds.add(blockId);
            blocks.add(block);
        }

        setRequestKey2BlockIds(key, blockIds);
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
