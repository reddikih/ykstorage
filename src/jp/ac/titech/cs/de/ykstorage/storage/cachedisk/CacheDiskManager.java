package jp.ac.titech.cs.de.ykstorage.storage.cachedisk;


import jp.ac.titech.cs.de.ykstorage.storage.Block;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.LRUBuffer;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.ReplacePolicy;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.dataplacement.PlacementPolicy;
import jp.ac.titech.cs.de.ykstorage.storage.diskstate.DiskStateType;
import jp.ac.titech.cs.de.ykstorage.storage.diskstate.StateManager;
import net.jcip.annotations.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class CacheDiskManager implements ICacheDiskManager {

    private final static Logger logger = LoggerFactory.getLogger(CacheDiskManager.class);

    // TODO to be pull up field
    private boolean deleteOnExit = false;

    private long capacity;

    @GuardedBy("this")
    private long size;

    private ReplacePolicy<Long> replacePolicy;

    private PlacementPolicy placementPolicy;

    private String deviceFilePrefix;

    private String diskFilePrefix;

    private int numberOfCacheDisks;

    private Map<Integer, DiskFileAndDevicePath> diskId2FilePath;

    private ExecutorService[] diskIOExecutors;

    private StateManager stateManager;

    private ReentrantReadWriteLock[] diskStateLocks;


    public CacheDiskManager(
            long capacity,
            int blockSize,
            int numberOfCacheDisks,
            String diskFilePrefix,
            String deviceFilePrefix,
            String[] deviceCharacters,
            PlacementPolicy placementPolicy,
            StateManager stateManager) {

        this.numberOfCacheDisks = numberOfCacheDisks;
        this.diskFilePrefix = diskFilePrefix;
        this.deviceFilePrefix = deviceFilePrefix;
        this.placementPolicy = placementPolicy;
        this.stateManager = stateManager;
        init(capacity, blockSize, deviceCharacters);
    }

    private void init(long capacity, int blockSize, String[] deviceCharacters) {
        int bufferSize = (int)Math.ceil((double)capacity / blockSize);
        this.capacity = bufferSize;
        this.replacePolicy = new LRUBuffer((int)this.capacity);

        this.diskId2FilePath = new HashMap<>();

        int diskId = 0;
        for (String deviceChar : deviceCharacters) {
            DiskFileAndDevicePath pathInfo = new DiskFileAndDevicePath(
                    this.diskFilePrefix + deviceChar + "/", this.deviceFilePrefix + deviceChar);
            diskId2FilePath.put(diskId++, pathInfo);
        }

        this.diskIOExecutors = new ExecutorService[this.numberOfCacheDisks];
        for (int i=0; i < numberOfCacheDisks; i++) {
            diskIOExecutors[i] = Executors.newFixedThreadPool(1);
        }

        this.diskStateLocks = new ReentrantReadWriteLock[numberOfCacheDisks];
        for (int i=0; i < this.diskStateLocks.length; i++) {
            this.diskStateLocks[i] = new ReentrantReadWriteLock();
        }
    }


    @Override
    public Block read(Long blockId) {
        Block result = null;

        try {
            result = readFromCacheDisk(blockId);
        } catch (ExecutionException e) {
            launderThrowable(e);
        } catch (InterruptedException e) {
            launderThrowable(e);
        }

        // Update internal LRU list
        if (result != null) {
            Long replaced = this.replacePolicy.add(result.getBlockId());
            if (replaced != null)
                logger.info("Replace blockId:{} with blockId:{} due to read", result.getBlockId(), blockId);
            else
                logger.info("Read from buffer. blockId:{}", blockId);
            return result;
        }

        return result;
    }

    @Override
    public Block write(Block block) {
        Block result;

        Long replaced;
        synchronized (this) {
            replaced = this.replacePolicy.add(block.getBlockId());
            this.size++;
        }
        if (replaced != null) {
            Block removed = null;
            synchronized (this) {
                this.size--;
                try {
                    removed = removeFromCacheDisk(replaced);
                } catch (IOException e) {
                    launderThrowable(e);
                }
            }
            result = removed;
        } else {
            try {
                writeToCacheDisk(block);
            } catch (ExecutionException e) {
                launderThrowable(e);
            } catch (InterruptedException e) {
                launderThrowable(e);
            }

            logger.info(
                    "Write to CacheDisk:{}. blockId:{} without replace",
                    assignPrimaryDiskId(block.getBlockId()), block.getBlockId());

            result = block;
        }

        return result;
    }

    private Block readFromCacheDisk(long blockId)
            throws ExecutionException, InterruptedException {

        Callable readTask = new ReadPrimitiveTask(
                blockId, getDiskFilePathPrefix(blockId));

        Future<byte[]> future =
                diskIOExecutors[assignPrimaryDiskId(blockId)].submit(readTask);
        byte[] payload = future.get();

        return new Block(blockId, 0, assignPrimaryDiskId(blockId), 0, payload);
    }

    private Block writeToCacheDisk(Block block)
            throws ExecutionException, InterruptedException {

        Callable writeTask = new WritePrimitiveTask(
                block, getDiskFilePathPrefix(block.getBlockId()));

        int diskId = assignPrimaryDiskId(block.getBlockId());

        Future<Boolean> future = diskIOExecutors[diskId].submit(writeTask);
        boolean result = future.get();

        if (result) {
            return block;
        } else {
            return null;
        }
    }

    private Block removeFromCacheDisk(long blockId) throws IOException {
        int diskId = assignPrimaryDiskId(blockId);

        File file = new File(getDiskFilePathPrefix(blockId) + blockId);
        if (!file.delete())
            throw new IOException("[" + file.getCanonicalPath() + "] is not exist or not a file.");

        logger.info("Removed successfully. blockId:{} from cache diskId:{}", blockId, diskId);

        return new Block(blockId, 0, assignPrimaryDiskId(blockId), 0, new byte[0]);
    }

    private int assignPrimaryDiskId(long blockId) {
        return this.placementPolicy.assignDiskId(blockId);
    }

    // TODO pull up
    public void checkDataDir(String dir) throws IOException {
        File file = new File(dir);
        if (deleteOnExit) file.deleteOnExit();

        if (!file.exists()) {
            if (!file.mkdirs())
                logger.info("could not create dir:{}", file.getCanonicalPath());
        }
    }

    private String getDiskFilePathPrefix(long blockId) {
        int diskId = assignPrimaryDiskId(blockId);
        DiskFileAndDevicePath pathInfo = this.diskId2FilePath.get(diskId);
        return pathInfo.getDiskFilePath();
    }

    // TODO pull up
    private RuntimeException launderThrowable(Throwable t) {
        if (t instanceof RuntimeException) return (RuntimeException) t;
        else if (t instanceof Error) throw (Error) t;
        else throw new IllegalStateException("Not unchecked", t);
    }



    ///////////----------  These are callable tasks  ----------///////////

    private class ReadPrimitiveTask implements Callable<byte[]> {

        private long blockId;
        private String diskFilePath;

        public ReadPrimitiveTask(long blockId, String diskFilePath) {
            this.blockId = blockId;
            this.diskFilePath = diskFilePath;
        }

        @Override
        public byte[] call() throws Exception {
            byte[] result = null;

            int diskId = assignPrimaryDiskId(blockId);
            diskStateLocks[diskId].readLock().lock();

            if (DiskStateType.ACTIVE.equals(stateManager.getState(diskId)) ||
                    DiskStateType.IDLE.equals(stateManager.getState(diskId))) {

                diskStateLocks[diskId].readLock().unlock();
                diskStateLocks[diskId].writeLock().lock();

                try {
                    File file = new File(this.diskFilePath + blockId);
                    if (!file.exists() || !file.isFile())
                        throw new IOException("[" + file.getCanonicalPath() + "] is not exist or not a file.");

                    result = new byte[(int)file.length()];

                    BufferedInputStream bis =
                            new BufferedInputStream(new FileInputStream(file));

                    if (bis.available() < 1)
                        throw new IOException("[" + this.diskFilePath + "] is not available.");

                    logger.info("Read blockId:{} from disk:{}", blockId, diskId);

                    stateManager.setState(diskId, DiskStateType.ACTIVE);

                    bis.read(result);
                    bis.close();

                    stateManager.setState(diskId, DiskStateType.IDLE);

                    logger.info("Read successfully. diskId:{} byte:{}",
                            file.getCanonicalPath(), file.length());

                } finally {
                    diskStateLocks[diskId].writeLock().unlock();
                }
            } else {
                logger.debug("Disk {} is not ACTIVE or IDLE state when to write to tha disk.", diskId);
            }

            try {} finally {
                if (diskStateLocks[diskId].getReadLockCount() > 0)
                    diskStateLocks[diskId].readLock().unlock();
            }

            return result;
        }

    }

    private class WritePrimitiveTask implements Callable<Boolean> {

        private Block block;
        private String diskFilePath;

        public WritePrimitiveTask(Block block, String diskFilePath) {
            this.block = block;
            this.diskFilePath = diskFilePath;
        }

        @Override
        public Boolean call() throws Exception {
            boolean result = false;

            int diskId = block.getPrimaryDiskId();
            diskStateLocks[diskId].readLock().lock();
            if (DiskStateType.ACTIVE.equals(stateManager.getState(diskId)) ||
                    DiskStateType.IDLE.equals(stateManager.getState(diskId))) {

                diskStateLocks[diskId].readLock().unlock();
                diskStateLocks[diskId].writeLock().lock();

                try {
                    File file = new File(diskFilePath + block.getBlockId());
                    if (deleteOnExit) file.deleteOnExit();

                    checkDataDir(file.getParent());

                    logger.info("write to: {}", file.getCanonicalPath());

                    if (!file.exists()) file.createNewFile();

                    BufferedOutputStream bos =
                            new BufferedOutputStream(new FileOutputStream(file));

                    stateManager.setState(diskId, DiskStateType.ACTIVE);
                    bos.write(this.block.getPayload());
                    bos.flush();
                    bos.close();

                    result = true;

                    stateManager.setState(diskId, DiskStateType.IDLE);

                    logger.info("Written successfully. diskId:{} byte:{}",
                            file.getCanonicalPath(), this.block.getPayload().length);

                } finally {
                    diskStateLocks[diskId].writeLock().unlock();
                }
            } else {
                logger.debug("Disk {} is not IDLE state when to write to tha disk.", diskId);
            }

            try {} finally {
                if (diskStateLocks[diskId].getReadLockCount() > 0)
                    diskStateLocks[diskId].readLock().unlock();
            }

            return result;
        }
    }

    // TODO pull up or to be external class.
    private class DiskFileAndDevicePath {
        private final String diskFilePath;
        private final String devicePath;

        DiskFileAndDevicePath(String fPath, String dPath) {
            this.diskFilePath = fPath;
            this.devicePath = dPath;
        }

        public String getDiskFilePath() {return diskFilePath;}
        public String getDevicePath() {return devicePath;}
    }

}
