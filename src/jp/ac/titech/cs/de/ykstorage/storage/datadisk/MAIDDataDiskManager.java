package jp.ac.titech.cs.de.ykstorage.storage.datadisk;

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
import jp.ac.titech.cs.de.ykstorage.storage.Block;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.dataplacement.PlacementPolicy;
import jp.ac.titech.cs.de.ykstorage.storage.diskstate.DiskStateType;
import jp.ac.titech.cs.de.ykstorage.storage.diskstate.IdleThresholdListener;
import jp.ac.titech.cs.de.ykstorage.storage.diskstate.StateManager;
import jp.ac.titech.cs.de.ykstorage.util.ObjectSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MAIDDataDiskManager implements IDataDiskManager, IdleThresholdListener {

    private final static Logger logger = LoggerFactory.getLogger(MAIDDataDiskManager.class);

    private final static String PLACEMENT_FILE_NAME = "maidddplacementpolicy";

    // TODO to be pull up field
    private boolean deleteOnExit = false;

    private boolean startedWatchdog = false;

    private String deviceFilePrefix;
    private String diskFilePrefix;
    private int numberOfDataDisks;
    private Map<Integer, DiskFileAndDevicePath> diskId2FilePath;

    private ExecutorService[] diskIOExecutors;

    private final ExecutorService diskOperationExecutor = Executors.newCachedThreadPool();

    private PlacementPolicy placementPolicy;

    private StateManager stateManager;

    private ReentrantReadWriteLock[] diskStateLocks;

    public MAIDDataDiskManager(
            int numberOfDataDisks,
            String diskFilePrefix,
            String deviceFilePrefix,
            String[] deviceCharacters,
            PlacementPolicy placementPolicy,
            StateManager stateManager) {

        this.numberOfDataDisks = numberOfDataDisks;
        this.diskFilePrefix = diskFilePrefix;
        this.deviceFilePrefix = deviceFilePrefix;
        this.placementPolicy = placementPolicy;
        this.stateManager = stateManager;
        init(deviceCharacters);
    }

    // TODO this method may need to pull up
    private void init(String[] deviceCharacters) {
        this.diskId2FilePath = new HashMap<>();

        int diskId = 0;
        for (String deviceChar : deviceCharacters) {
            DiskFileAndDevicePath pathInfo = new DiskFileAndDevicePath(
                    this.diskFilePrefix + deviceChar + "/", this.deviceFilePrefix + deviceChar);
            diskId2FilePath.put(diskId++, pathInfo);
        }

        for (DiskFileAndDevicePath pathInfo : diskId2FilePath.values()) {
            logger.debug("[DataDisk] {}", pathInfo.getDiskFilePath());
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


        PlacementPolicy savedPolicy =
                new ObjectSerializer<PlacementPolicy>().deSerializeObject(PLACEMENT_FILE_NAME);
        if (savedPolicy != null) {
            this.placementPolicy = savedPolicy;
            logger.info("Reloaded saved placement policy object: {}", PLACEMENT_FILE_NAME);
        } else {
            logger.info("Unloaded saved placement policy object: {}", PLACEMENT_FILE_NAME);
        }

    }

    public void startWatchDog() {
        if (this.startedWatchdog) return;

        for (int i=0; i<numberOfDataDisks; i++) {
            this.stateManager.startIdleStateWatchDog(i);
        }
        this.startedWatchdog = true;
    }

    public List<Block> read(List<Long> blockIds) {
        List<Block> result = new ArrayList<>();

        List<OperationTask> operations = new ArrayList<>();
        for (Long blockId : blockIds)
            operations.add(new OperationTask(blockId, IOType.READ));

        try {
            List<Future<Object>> futures =
                    this.diskOperationExecutor.invokeAll(operations);

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

    private boolean spinDown(int diskId) {
        logger.debug("Spin-down start. diskId:{}", diskId);

        String devicePath = this.diskId2FilePath.get(diskId).getDevicePath();
        if (!devicePathCheck(devicePath)) return false;

        String command = "sync";
        int rc = executeExternalCommand(command);
        logger.debug("return value of [{}]: {}", command, rc);

        command = "sudo hdparm -y " + devicePath;
        rc = executeExternalCommand(command);
        logger.debug("return value of [{}]: {}", command, rc);
        if (rc != 0) return false;
        // TODO increment spin down count.
        // and the other some operation if needed.

        logger.debug("Spin-down end. diskId:{} path:{}", diskId, devicePath);

        return true;
    }

    private boolean spinUp(int diskId) {
        logger.debug("Spin-up start. diskId:{}", diskId);

        String devicePath = this.diskId2FilePath.get(diskId).getDevicePath();
        if (!devicePathCheck(devicePath)) return false;

        String command = "ls " + devicePath;
        int rc = executeExternalCommand(command);
        logger.debug("return value of [{}]: {}", command, rc);
        if (rc != 0) return false;
        // TODO increment spin up count.
        // and the other some operation if needed.

        logger.debug("Spin-up end. diskId:{} path:{}", diskId, devicePath);

        return true;
    }

    private int executeExternalCommand(String command) {
        int returnCode = 1;

        try {
            Process process = Runtime.getRuntime().exec(command);
            returnCode = process.waitFor();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("command:{} has faced exception:{} exception message: {}",
                    command, e.getClass().getSimpleName(), e.getMessage());
            launderThrowable(e);
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error("command:{} has faced exception:{} exception message: {}",
                    command, e.getClass().getSimpleName(), e.getMessage());
            launderThrowable(e);
        }
        return returnCode;
    }

    private boolean devicePathCheck(String devicePath) {
        if (devicePath == null || devicePath == "") {
            return false;
        }

        File file = new File(devicePath);
        return file.exists();
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
        return this.placementPolicy.assignDiskId(blockId);
    }

    // TODO pull up
    private RuntimeException launderThrowable(Throwable t) {
        if (t instanceof RuntimeException) return (RuntimeException) t;
        else if (t instanceof Error) throw (Error) t;
        else throw new IllegalStateException("Not unchecked", t);
    }

    // TODO pull up
    @Override
    public void setDeleteOnExit(boolean deleteOnExit) {
        this.deleteOnExit = deleteOnExit;
    }

    @Override
    public void termination() {
        ObjectSerializer<PlacementPolicy> serializer = new ObjectSerializer<>();
        serializer.serializeObject(this.placementPolicy, PLACEMENT_FILE_NAME);
    }

    @Override
    public void exceededIdleThreshold(int diskId) {
        logger.debug("diskId: {} is exceeded idleness threshold time", diskId);
        diskStateLocks[diskId].readLock().lock();
        if (DiskStateType.IDLE.equals(stateManager.getState(diskId))) {
            diskStateLocks[diskId].readLock().unlock();
            diskStateLocks[diskId].writeLock().lock();
            try {
                stateManager.setState(diskId, DiskStateType.SPINDOWN);
                if (spinDown(diskId)) {
                    stateManager.setState(diskId, DiskStateType.STANDBY);
                    logger.debug("Spinning down diskId:{} is successful.", diskId);
                } else {
                    stateManager.setState(diskId, DiskStateType.IDLE);
                    logger.debug("Spinning down diskId:{} is failed. and return state to IDLE", diskId);
                }
            } finally {
                diskStateLocks[diskId].writeLock().unlock();
            }
        }

        try {} finally {
            if (diskStateLocks[diskId].getReadLockCount() > 0)
                diskStateLocks[diskId].readLock().unlock();
        }
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
                } else {
                    result = new Boolean(false);
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

            logger.debug("[Read Primitive] start");

            byte[] result = null;

            int diskId = assignPrimaryDiskId(blockId);
            diskStateLocks[diskId].readLock().lock();

            logger.debug("Read lock. disk state:{} diskId:{}", stateManager.getState(diskId), diskId);

            if (DiskStateType.STANDBY.equals(stateManager.getState(diskId)) ||
                    DiskStateType.SPINDOWN.equals(stateManager.getState(diskId))) {

                diskStateLocks[diskId].readLock().unlock();
                diskStateLocks[diskId].writeLock().lock();

                try {
                    stateManager.setState(diskId, DiskStateType.SPINUP);
                    if (!spinUp(diskId))
                        throw new IllegalStateException(
                                "Couldn't spin up the disk id: " + diskId);

                    stateManager.setState(diskId, DiskStateType.IDLE);

                } finally {
                    diskStateLocks[diskId].writeLock().unlock();
                }
            }

            try {} finally {
                if (diskStateLocks[diskId].getReadLockCount() > 0)
                    diskStateLocks[diskId].readLock().unlock();
            }


            // when the disk is spinning then we can read the data from it.
            // and update the disk status IDLE to ACTIVE
            diskStateLocks[diskId].readLock().lock();

            logger.debug("Read lock. disk state:{} diskId:{}", stateManager.getState(diskId), diskId);

            if (DiskStateType.ACTIVE.equals(stateManager.getState(diskId)) ||
                    DiskStateType.IDLE.equals(stateManager.getState(diskId)) ||
                    DiskStateType.SPINUP.equals(stateManager.getState(diskId))) {

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

                    stateManager.setState(diskId, DiskStateType.ACTIVE);

                    bis.read(result);
                    bis.close();

                    logger.info("Read a block from:{}. DataDiskId:{} Byte:{}",
                            file.getCanonicalPath(), diskId, file.length());

                    stateManager.setState(diskId, DiskStateType.IDLE);
                    stateManager.resetWatchDogTimer(diskId);
                    stateManager.startIdleStateWatchDog(diskId);

                } finally {
                    diskStateLocks[diskId].writeLock().unlock();
                }
            } else {
                logger.debug("Disk {} is not ACTIVE or IDLE state when to write to tha disk. It is [{}]", diskId, stateManager.getState(diskId));
            }

            try {} finally {
                if (diskStateLocks[diskId].getReadLockCount() > 0)
                    diskStateLocks[diskId].readLock().unlock();
            }

            logger.debug("[Read Primitive] end");

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

            logger.debug("[Write Primitive] start");

            boolean result = false;

            int diskId = block.getPrimaryDiskId();
            diskStateLocks[diskId].readLock().lock();

            logger.debug("Read lock. disk state:{} diskId:{}", stateManager.getState(diskId), diskId);

            if (DiskStateType.STANDBY.equals(stateManager.getState(diskId)) ||
                    DiskStateType.SPINDOWN.equals(stateManager.getState(diskId))) {

                diskStateLocks[diskId].readLock().unlock();
                diskStateLocks[diskId].writeLock().lock();

                try {
                    stateManager.setState(diskId, DiskStateType.SPINUP);
                    if (!spinUp(diskId))
                        throw new IllegalStateException(
                                "Couldn't spin up the disk id: " + diskId);

                    stateManager.setState(diskId, DiskStateType.IDLE);

                } finally {
                    diskStateLocks[diskId].writeLock().unlock();
                }
            }

            try {} finally {
                if (diskStateLocks[diskId].getReadLockCount() > 0)
                    diskStateLocks[diskId].readLock().unlock();
            }


            diskStateLocks[diskId].readLock().lock();

            logger.debug("Read lock. disk state:{} diskId:{}", stateManager.getState(diskId), diskId);

            if (DiskStateType.ACTIVE.equals(stateManager.getState(diskId)) ||
                    DiskStateType.IDLE.equals(stateManager.getState(diskId)) ||
                    DiskStateType.SPINUP.equals(stateManager.getState(diskId))) {

                diskStateLocks[diskId].readLock().unlock();
                diskStateLocks[diskId].writeLock().lock();

                try {
                    File file = new File(diskFilePath + block.getBlockId());
                    if (deleteOnExit) file.deleteOnExit();

                    checkDataDir(file.getParent());

                    if (!file.exists()) file.createNewFile();

                    BufferedOutputStream bos =
                            new BufferedOutputStream(new FileOutputStream(file));

                    stateManager.setState(diskId, DiskStateType.ACTIVE);
                    bos.write(this.block.getPayload());
                    bos.flush();
                    bos.close();

                    result = true;

                    logger.info("Written a block to:{}. DataDiskId:{} byte:{}",
                            file.getCanonicalPath(), diskId, this.block.getPayload().length);

                    stateManager.setState(diskId, DiskStateType.IDLE);
                    stateManager.resetWatchDogTimer(diskId);
                    stateManager.startIdleStateWatchDog(diskId);

                } finally {
                    diskStateLocks[diskId].writeLock().unlock();
                }
            } else {
                logger.debug("Disk {} is not ACTIVE or IDLE state when to write to tha disk. It is [{}]", diskId, stateManager.getState(diskId));
            }

            try {} finally {
                if (diskStateLocks[diskId].getReadLockCount() > 0)
                    diskStateLocks[diskId].readLock().unlock();
            }

            logger.debug("[Write Primitive] end");

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
