package jp.ac.titech.cs.de.ykstorage.storage.datadisk;

import jp.ac.titech.cs.de.ykstorage.storage.Block;
import jp.ac.titech.cs.de.ykstorage.storage.diskstate.DiskStateType;
import jp.ac.titech.cs.de.ykstorage.storage.diskstate.IdleThresholdListener;
import jp.ac.titech.cs.de.ykstorage.storage.diskstate.StateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class NormalDataDiskManager implements IDataDiskManager, IdleThresholdListener {

    private final static Logger logger = LoggerFactory.getLogger(NormalDataDiskManager.class);

    // TODO to be pull up field
    private boolean deleteOnExit = false;

    private String deviceFilePrefix;
    private String diskFilePrefix;
    private int numberOfDataDisks;
    private Map<Integer, DiskFileAndDevicePath> diskId2FilePath;

    private ExecutorService[] diskIOExecutors;

    private final ExecutorService diskOperationExecutor = Executors.newCachedThreadPool();

    private StateManager stateManager;

    private ReentrantReadWriteLock[] diskStateLocks;

    public NormalDataDiskManager(
            int numberOfDataDisks,
            String diskFilePrefix,
            String deviceFilePrefix,
            String[] deviceCharacters,
            StateManager stateManager) {

        this.diskFilePrefix = diskFilePrefix;
        this.numberOfDataDisks = numberOfDataDisks;
        this.deviceFilePrefix = deviceFilePrefix;
        this.stateManager = stateManager;
        init(deviceCharacters);
    }

    // TODO this method may need to pull up
    private void init(String[] deviceCharacters) {
        this.diskId2FilePath = new HashMap<>();

        int diskId= 0;
        for (String deviceChar : deviceCharacters) {
            DiskFileAndDevicePath pathInfo = new DiskFileAndDevicePath(
                    this.diskFilePrefix + deviceChar + "/", this.deviceFilePrefix + deviceChar);
            diskId2FilePath.put(diskId++, pathInfo);
        }

        this.diskIOExecutors = new ExecutorService[this.numberOfDataDisks];
        for (int i=0; i < numberOfDataDisks; i++) {
            diskIOExecutors[i] = Executors.newFixedThreadPool(1);
        }

        this.diskStateLocks = new ReentrantReadWriteLock[numberOfDataDisks];
        for (int i=0; i < this.diskStateLocks.length; i++) {
            this.diskStateLocks[i] = new ReentrantReadWriteLock();
        }

        // register watchdog of idleness of data disks.
        this.stateManager.addListener(this);
    }

    @Override
    public List<Block> read(List<Long> blockIds) {
        List<Block> result = new ArrayList<>();

        List<OperationTask> operations = new ArrayList<>();
        for (Long blockId : blockIds)
            operations.add(new OperationTask(blockId, IOType.READ));

        try {
            List<Future<Object>> futures = this.diskOperationExecutor.invokeAll(operations);
            for (Future f : futures) {
                result.add((Block)f.get());
            }

        } catch (InterruptedException e) {
            throw launderThrowable(e);
        } catch (ExecutionException e) {
            throw launderThrowable(e);
        }

        return result;
    }

    @Override
    public boolean write(List<Block> blocks) {
        boolean result = true;

        List<OperationTask> operations = new ArrayList<>();
        for (Block block : blocks)
            operations.add(new OperationTask(block, IOType.WRITE));

        try {
            List<Future<Object>> futures = this.diskOperationExecutor.invokeAll(operations);
            for (Future f : futures)
                if (!(Boolean)f.get() && result == true)
                    result = false;

        } catch (InterruptedException e) {
            throw launderThrowable(e);
        } catch (ExecutionException e) {
            throw launderThrowable(e);
        }

        return result;
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

    @Override
    public int assignPrimaryDiskId(long blockId) {
        BigInteger numerator = BigInteger.valueOf(blockId);
        BigInteger denominator = BigInteger.valueOf(this.numberOfDataDisks);
        return numerator.mod(denominator).intValue();
    }

    // TODO pull up
    private RuntimeException launderThrowable(Throwable t) {
        if (t instanceof RuntimeException) return (RuntimeException) t;
        else if (t instanceof Error) throw (Error) t;
        else throw new IllegalStateException("Not unchecked", t);
    }

    // TODO pull up
    public void setDeleteOnExit(boolean deleteOnExit) {
        this.deleteOnExit = deleteOnExit;
    }

    @Override
    public void exceededIdleThreshold(int diskId) {
        logger.debug("diskId: {} is exceeded idleness threshold time but Normal will no operation.", diskId);
    }

    private String getDiskFilePathPrefix(long blockId) {
        int diskId = assignPrimaryDiskId(blockId);
        DiskFileAndDevicePath pathInfo = this.diskId2FilePath.get(diskId);
        return pathInfo.getDiskFilePath();
    }


    private class OperationTask implements Callable<Object> {

        private long blockId;
        private Block block;
        private IOType ioType;

        /**
         * this constructor be only used by read request.
         *
         * @param blockId
         * @param ioType
         */
        public OperationTask(long blockId, IOType ioType) {
            this(blockId, null, ioType);
        }

        /**
         * this constructor be only used by write request.
         *
         * @param block
         * @param ioType
         */
        public OperationTask(Block block, IOType ioType) {
            this(-1, block, ioType);
        }

        private OperationTask(long blockId, Block block, IOType ioType) {
            this.blockId = blockId;
            this.block = block;
            this.ioType = ioType;
        }

        @Override
        public Object call() throws Exception {
            Object result = null;
            if (ioType.equals(IOType.READ)) {
                Callable readTask = new ReadPrimitiveTask(blockId, getDiskFilePathPrefix(blockId));

                Future<byte[]> future = diskIOExecutors[assignPrimaryDiskId(blockId)].submit(readTask);
                byte[] payload = future.get();

                result = new Block(blockId, 0, assignPrimaryDiskId(blockId), 0, payload);
            } else if (ioType.equals(IOType.WRITE)) {
                Callable writeTask = new WritePrimitiveTask(block, getDiskFilePathPrefix(block.getBlockId()));

                Future<Boolean> future = diskIOExecutors[block.getPrimaryDiskId()].submit(writeTask);
                boolean isWritten = future.get();
                if (isWritten) {
                    result = new Boolean(true);
                }
            }

            return result;
        }
    }

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

    // TODO pull up or to be external class.
    private enum IOType {
        READ,
        WRITE,
    }
}
