package jp.ac.titech.cs.de.ykstorage.storage;


import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.IBufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.RAPoSDABufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.ICacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.IDataDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.RAPoSDADataDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.diskstate.DiskStateType;
import jp.ac.titech.cs.de.ykstorage.util.ObjectSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class RAPoSDAStorageManager extends StorageManager {

    private final static Logger logger = LoggerFactory.getLogger(RAPoSDAStorageManager.class);

    private HashMap<Long, List<List<Long>>> key2blockIdMap = new HashMap<>();

    private final String KEY_2_BLOCKID_MAP_NAME = "raposdakey2blockidmap";

    private int numberOfReplica;

    public RAPoSDAStorageManager(
            IBufferManager bufferManager,
            ICacheDiskManager cacheDiskManager,
            IDataDiskManager dataDiskManager,
            Parameter parameter,
            int numberOfReplica) {
        super(bufferManager, cacheDiskManager, dataDiskManager, parameter);
        init(numberOfReplica);
    }

    private void init(int numberOfReplica) {
        this.numberOfReplica = numberOfReplica;

        HashMap<Long, List<List<Long>>> savedMap =
                new ObjectSerializer<HashMap>().deSerializeObject(KEY_2_BLOCKID_MAP_NAME);
        if (savedMap != null) {
            this.key2blockIdMap = savedMap;
            logger.info("Saved key to blockId mapping file is reloaded: {}", KEY_2_BLOCKID_MAP_NAME);
        } else {
            logger.info("Unloaded saved key to blockId mapping file: {}", KEY_2_BLOCKID_MAP_NAME);
        }

        watchdogStart();
    }

    public void watchdogStart() {
        //TODO consider refactoring of following code: explicit type casting.
        ((RAPoSDADataDiskManager)this.dataDiskManager).startWatchDog();
    }

    @Override
    public byte[] read(long key) {
        byte[] result = null;
        List<List<Long>> blockIds = getCorrespondingBlockIds(key);

        if (blockIds == null || blockIds.size() == 0) {
            logger.debug("Key:{} has not assigned to any blocks yet.", key);
            return null;
        }

        ExecutorService executor = Executors.newCachedThreadPool();

        List<ReadReplicaSetTask> tasks = new ArrayList<>();

        for (List<Long> replicas : blockIds) {
            tasks.add(new ReadReplicaSetTask(replicas));
        }

        try {
            List<Future<Block>> futures = executor.invokeAll(tasks);

            // 読み出したブロックの集約
            List<byte[]> readPayloads = new ArrayList<>();
            int length = 0;
            for (Future<Block> future : futures) {
                Block block = future.get();

                if (block.getPayload() == null) {
                    logger.error("Read block's payload is null. BlockId:{} and set dummy payload to continue this process.", block.getBlockId());
                    // TODO to be fixed this defect near future.
//                    throw new IllegalStateException("Read block's payload is null. BlockId:" + block.getBlockId());
                    block.setPayload(new byte[Block.BLOCK_SIZE]);
                }

                length += block.getPayload().length;
                readPayloads.add(block.getPayload());
            }

            ByteBuffer buffer = ByteBuffer.allocate(length);

            for (byte[] payload : readPayloads) {
                buffer.put(payload);
            }

            result = buffer.array();
        } catch (InterruptedException e) {
            launderThrowable(e);
        } catch (ExecutionException e) {
            launderThrowable(e);
        }

        return result;
    }

    private class ReadReplicaSetTask implements Callable<Block> {

        private final List<Long> blockIds;
        private final int primaryDiskId;

        public ReadReplicaSetTask(List<Long> blockIds) {
            this.blockIds = blockIds;
            primaryDiskId = assignPrimaryDisk(blockIds.get(0));
        }

        @Override
        public Block call() throws Exception {

            Block result = null;

            // Read from buffer
            for (int i=0; i<blockIds.size(); i++) {
                result = ((RAPoSDABufferManager)bufferManager)
                        .read(new Block(blockIds.get(i), i, primaryDiskId, -1, null));
                if (result != null) break;
            }
            if (result != null) return result;


            // Read from cache disk
            result = cacheDiskManager.read(blockIds.get(0));
            if (result != null) return result;


            // read from data disks
            HashMap<Integer, Long> did2bid = new HashMap<>();
            List<Integer> diskIds = new ArrayList<>();
            HashMap<Long, Integer> bid2repLevel = new HashMap<>();
            for (int i=0; i<blockIds.size(); i++) {
                int diskId = assignReplicaDiskId(primaryDiskId, i);
                diskIds.add(diskId);
                did2bid.put(diskId, blockIds.get(i));
                bid2repLevel.put(blockIds.get(i), i);
            }

            List<Integer> activeDiskIds = getSpinningDiskIds(diskIds);

            // case 1. one of N disks is active or idle
            if (activeDiskIds.size() == 1) {
                logger.debug("[Read from DataDisk] case 1.one of N disks is active or idle. diskId:{} N:{}", activeDiskIds.get(0), parameter.numberOfDataDisks);

                long blockId = did2bid.get(activeDiskIds.get(0));
                Block block = new Block(
                        blockId,
                        bid2repLevel.get(blockId),
                        primaryDiskId,
                        -1,
                        null);

                ((RAPoSDADataDiskManager)dataDiskManager).spinUpDiskIfSleeping(
                        assignReplicaDiskId(block.getPrimaryDiskId(), block.getReplicaLevel()));

                result = ((RAPoSDADataDiskManager)dataDiskManager).read(block);
            }

            // case 2. M of N disks are active or idle (0 < M <= N)
            else if (0 < activeDiskIds.size() && activeDiskIds.size() <= diskIds.size()) {
                logger.debug("[Read from DataDisk] M of N disks are active or idle (0 < M <= N) M:{} N:{}", activeDiskIds.size(), parameter.numberOfDataDisks);

                int maximumLengthDiskId = -1;
                int maximumBufferLength = -1;

                for (int diskId : activeDiskIds) {
                    int bufferLength = (((RAPoSDABufferManager)bufferManager)
                            .getBufferLengthCorrespondingToSpecifiedDisk(diskId));

                    if (maximumBufferLength < bufferLength) {
                        maximumBufferLength = bufferLength;
                        maximumLengthDiskId = diskId;
                    }
                }

                ((RAPoSDADataDiskManager)dataDiskManager).spinUpDiskIfSleeping(maximumLengthDiskId);

                long blockId = did2bid.get(maximumLengthDiskId);
                Block block = new Block(
                        blockId,
                        bid2repLevel.get(blockId),
                        primaryDiskId,
                        -1,
                        null);
                result = ((RAPoSDADataDiskManager)dataDiskManager).read(block);
            }

            // case 3. all of N disks are standby
            else {

                logger.debug("[Read from DataDisk] case 3. all of N disks are standby. N:{}", activeDiskIds.size(), parameter.numberOfDataDisks);

                // 停止期間の長いディスクから
                int longestSleepingDiskId = -1;
                long longestSleepingTime = -1L;

                for (int diskId : diskIds) {
                    long sleepingTime = ((RAPoSDADataDiskManager)dataDiskManager).getSleepingTimeByDiskId(diskId);
                    if (longestSleepingTime < sleepingTime) {
                        longestSleepingDiskId = diskId;
                        longestSleepingTime = sleepingTime;
                    }
                }

                ((RAPoSDADataDiskManager)dataDiskManager).spinUpDiskIfSleeping(longestSleepingDiskId);

                long blockId = did2bid.get(longestSleepingDiskId);
                Block block = new Block(
                        blockId,
                        bid2repLevel.get(blockId),
                        primaryDiskId,
                        -1,
                        null);
                result = ((RAPoSDADataDiskManager)dataDiskManager).read(block);
            }

            // write to cache disk
            List<Block> toCacheDisk = new ArrayList<>();
            toCacheDisk.add(result);
            writeToCacheDisk(toCacheDisk);

            return result;
        }
    }

    private List<Integer> getSpinningDiskIds(List<Integer> diskIds) {
        List<Integer> result = new ArrayList<>();
        for (int diskId : diskIds) {
            if (DiskStateType.IDLE.equals(((RAPoSDADataDiskManager)dataDiskManager).getState(diskId)) ||
                    DiskStateType.ACTIVE.equals(((RAPoSDADataDiskManager)dataDiskManager).getState(diskId))) {
                result.add(diskId);
            }
        }
        return result;
    }

    @Override
    public boolean write(long key, byte[] value) {
        List<List<Long>> blockIds = getCorrespondingBlockIds(key);

        if (blockIds == null || blockIds.size() == 0) {
            blockIds = assignBlockIds(
                    key, value.length, Block.BLOCK_SIZE, this.numberOfReplica);
            setRequestKey2BlockIds(key, blockIds);
        }

        // Create blocks for each replica
        List<List<Block>> blocks =
                createReplicatedBlocks(blockIds, value, Block.BLOCK_SIZE);

        // Write to the buffer for each block
        for (List<Block> replicas : blocks) {
            for (Block block : replicas) {
                Block result = this.bufferManager.write(block);

                if (result == null) {

                    //// Overflow process ////

                    int overflowedBufferId =
                            ((RAPoSDABufferManager)bufferManager)
                                    .getCorrespondingBufferId(block);

                    int maximumBufferedDiskId =
                            ((RAPoSDABufferManager)bufferManager)
                                    .getMaximumBufferLengthDiskId(
                                            overflowedBufferId, block.getReplicaLevel());

                    if (maximumBufferedDiskId < 0) {
                        logger.debug(
                                "maximumBufferedDiskId is invalid:{} set its value to block's ownerDiskId:{}",
                                maximumBufferedDiskId, block.getOwnerDiskId());
                        maximumBufferedDiskId = block.getOwnerDiskId();
                    }

                    ((RAPoSDADataDiskManager)dataDiskManager).spinUpDiskIfSleeping(maximumBufferedDiskId);

                    Set<Block> toBeFlushedBlocks = new HashSet<>();
                    toBeFlushedBlocks.addAll(
                            ((RAPoSDABufferManager)bufferManager).getBlocksInTheSameRegion(block)
                    );

                    List<Block> correspondingBlocks =
                            ((RAPoSDABufferManager)bufferManager)
                                    .getBlocksCorrespondingToSpecifiedDisk(maximumBufferedDiskId);

                    toBeFlushedBlocks.addAll(correspondingBlocks);

                    List<Block> toBeRemoved =
                            ((RAPoSDADataDiskManager) dataDiskManager).writeBlocks(toBeFlushedBlocks);

                    if (toBeRemoved != null) {
                        for (Block toRemove : toBeRemoved) {
                            Block removed = ((RAPoSDABufferManager)bufferManager).remove(toRemove);
                            if (removed == null) {
                                logger.info("Removed block is missing in the buffer.");
                            }
                        }
                    }

                    Block written = this.bufferManager.write(block);
                    if (written == null)
                        logger.debug("Write block(Id):{} is failed.", block.getBlockId());

                    // When the blocks is flushed to data disks, then these are
                    // written to the cache disks asynchronously.
                    writeToCacheDisk(toBeRemoved);
                }
            }
        }

        return true;
    }

    @Override
    public void shutdown() {
        ObjectSerializer<HashMap> serializer = new ObjectSerializer<>();
        serializer.serializeObject(this.key2blockIdMap, KEY_2_BLOCKID_MAP_NAME);
        this.dataDiskManager.termination();
        logger.info("Done the termination process.");
    }

    private void writeToCacheDisk(final List<Block> toBeFlushedBlocks) {
        Runnable cacheDiskWriter = new Runnable() {
            @Override
            public void run() {
                for (Block block : toBeFlushedBlocks) {
                    // write the block which is only primary block.
                    if (block.getReplicaLevel() == 0)
                        try {
                            cacheDiskManager.write(block);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                }
            }
        };

        Executor executor = Executors.newSingleThreadExecutor();
        executor.execute(cacheDiskWriter);
    }


    // TODO pull up method
    private List<List<Long>> getCorrespondingBlockIds(long key) {
        return this.key2blockIdMap.get(key);
    }

    private List<List<Long>> assignBlockIds(
            long key, int byteSize, int blockSize, int numberOfReplica) {

        List<List<Long>> blockIds = new ArrayList<>();

        int numBlocks = (int)Math.ceil((double)byteSize / blockSize);
        for (int i=0; i<numBlocks; i++) {
            List<Long> replicaIds =new ArrayList<>();

            long primaryBlockId = -1;
            for (int j=0; j<numberOfReplica; j++) {
                long blockId = this.sequenceNumber.getAndIncrement();

                if (j == 0) primaryBlockId = blockId;
                replicaIds.add(blockId);
                logger.info("Assigned blockId. key:{} blockId:{} replicaLevel:{} primaryBlockId:{}",
                        key, blockId, j, primaryBlockId);
            }
            blockIds.add(replicaIds);
        }
        return blockIds;
    }


    // TODO pull up method
    private void setRequestKey2BlockIds(long key, List<List<Long>> blockIds) {
        this.key2blockIdMap.put(key, blockIds);
    }

    private List<List<Block>> createReplicatedBlocks(
            List<List<Long>> blockIds, byte[] value, int blockSize) {

        ArrayList<List<Block>> replicatedBlocks = new ArrayList<>();
        Iterator<byte[]> dividedValue =
                divideValue(blockIds.size(), blockSize, value).iterator();

        for (List<Long> replicaIds : blockIds) {
            List<Block> replicas = createReplicas(replicaIds, dividedValue.next());
            replicatedBlocks.add(replicas);
        }

        return replicatedBlocks;
    }

    private List<Block> createReplicas(List<Long> replicaIds, byte[] value) {
        List<Block> replicas = new ArrayList<>();

        int primaryDiskId = assignPrimaryDisk(replicaIds.get(0));

        Iterator<Long> iter = replicaIds.iterator();
        for (int i=0; i<replicaIds.size(); i++) {
            long blockId = iter.next();

            byte[] copyValue = Arrays.copyOf(value, value.length);

            Block block = new Block(
                    blockId,
                    i,
                    primaryDiskId,
                    -1, // Is it required ?
                    assignReplicaDiskId(primaryDiskId, i),
                            copyValue);

            logger.info("Create block. {}", block.toString());

            replicas.add(block);
        }
        return replicas;
    }

    private List<byte[]> divideValue(int denominator, int blockSize, byte[] value) {
        List<byte[]> result = new ArrayList<>();
        for (int i=0; i<denominator; i++) {
            byte[] payload = new byte[blockSize];

            int length = value.length - i * blockSize < blockSize
                    ? value.length - i * blockSize : blockSize;

            System.arraycopy(value, i * blockSize, payload, 0, length);
            result.add(payload);
        }
        return result;
    }

    // TODO pull up method
    private int assignPrimaryDisk(long blockId) {
        return this.dataDiskManager.assignPrimaryDiskId(blockId);
    }

    private int assignReplicaDiskId(int primaryDiskId, int replicaLevel) {
        return ((RAPoSDADataDiskManager)dataDiskManager).assignReplicaDiskId(primaryDiskId, replicaLevel);
    }

    // TODO pull up
    private RuntimeException launderThrowable(Throwable t) {
        if (t instanceof RuntimeException) return (RuntimeException) t;
        else if (t instanceof Error) throw (Error) t;
        else throw new IllegalStateException("Not unchecked", t);
    }

}
