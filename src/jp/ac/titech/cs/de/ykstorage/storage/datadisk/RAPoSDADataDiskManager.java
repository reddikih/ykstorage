package jp.ac.titech.cs.de.ykstorage.storage.datadisk;

import jp.ac.titech.cs.de.ykstorage.storage.Block;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.dataplacement.PlacementPolicy;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.replication.ReplicationPolicy;
import jp.ac.titech.cs.de.ykstorage.storage.diskstate.DiskStateType;
import jp.ac.titech.cs.de.ykstorage.storage.diskstate.IdleThresholdListener;
import jp.ac.titech.cs.de.ykstorage.storage.diskstate.StateManager;
import jp.ac.titech.cs.de.ykstorage.util.ObjectSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RAPoSDADataDiskManager implements IDataDiskManager, IdleThresholdListener {

    private final static Logger logger = LoggerFactory.getLogger(RAPoSDADataDiskManager.class);

    private final static String PLACEMENT_FILE_NAME = "raposdaddplacementpolicy";

    // TODO clean up
    private native boolean write(String filePath, byte[] value);
    private native byte[] read(String filePath);
    static {
        System.loadLibrary("raposdadatadiskio");
    }

    // TODO to be pull up field
    private boolean deleteOnExit = false;

    private boolean startedWatchdog = false;

    private String deviceFilePrefix;

    private String diskFilePrefix;

    private int numberOfDataDisks;

    private Map<Integer, DiskFileAndDevicePath> diskId2FilePath = new HashMap<>();

    private ExecutorService[] diskIOExecutors;

    private final ExecutorService diskOperationExecutor = Executors.newCachedThreadPool();

    private StateManager stateManager;

    private ReentrantReadWriteLock[] diskStateLocks;

    private PlacementPolicy placementPolicy;

    private ReplicationPolicy replicationPolicy;


    public RAPoSDADataDiskManager(
            int numberOfDataDisks,
            String diskFilePrefix,
            String deviceFilePrefix,
            String[] deviceCharacters,
            PlacementPolicy placementPolicy,
            ReplicationPolicy replicationPolicy,
            StateManager stateManager) {

        this.numberOfDataDisks = numberOfDataDisks;
        this.diskFilePrefix = diskFilePrefix;
        this.deviceFilePrefix = deviceFilePrefix;
        this.placementPolicy = placementPolicy;
        this.replicationPolicy = replicationPolicy;
        this.stateManager = stateManager;
        init(deviceCharacters);
    }

    // TODO this method may need to pull up
    private void init(String[] deviceCharacters) {
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

    @Override
    @Deprecated
    public List<Block> read(List<Long> blockIds) {
        throw new IllegalAccessError("This method shouldn't be use in RAPoSDA storage.");
    }

    public Block read(Block block) {
        Block result;

        Callable<Object> operation = new OperationTask(block, IOType.READ);

        try {
            Future<Object> future =
                    this.diskOperationExecutor.submit(operation);
            result = (Block)future.get();

        } catch (InterruptedException e) {
            throw launderThrowable(e);
        } catch (ExecutionException e) {
            throw launderThrowable(e);
        }

        return result;
    }

    @Override
    @Deprecated
    public boolean write(List<Block> blocks) {
        throw new IllegalAccessError("This method shouldn't be use in RAPoSDA storage.");
    }

    /**
     *
     * @param blocks
     * @return A block list that includes blocks written to data disks successfully.
     */
    public List<Block> writeBlocks(Collection<Block> blocks) {
        List<Block> result = new ArrayList<>();

        List<OperationTask> operations = new ArrayList<>();
        for (Block block : blocks)
            operations.add(new OperationTask(block, IOType.WRITE));

        try {
            List<Future<Object>> futures =
                    this.diskOperationExecutor.invokeAll(operations);

            for (Future f : futures)
                result.add((Block)f.get());

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
        if (rc != 0) {
            return false;
        }
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

    public void spinUpDiskIfSleeping(int diskId) {
        diskStateLocks[diskId].readLock().lock();
        logger.debug("Locked readLock. disk state:{} diskId:{}", stateManager.getState(diskId), diskId);

        if (DiskStateType.STANDBY.equals(stateManager.getState(diskId)) ||
                DiskStateType.SPINDOWN.equals(stateManager.getState(diskId))) {
            diskStateLocks[diskId].readLock().unlock();
            logger.debug("Unlocked readLock. disk state:{} diskId:{}", stateManager.getState(diskId), diskId);

            diskStateLocks[diskId].writeLock().lock();
            logger.debug("Locked writeLock. disk state:{} diskId:{}", stateManager.getState(diskId), diskId);

            try {
                stateManager.setState(diskId, DiskStateType.SPINUP);
                if (spinUp(diskId)) {
                    stateManager.setState(diskId, DiskStateType.IDLE);
                    stateManager.resetWatchDogTimer(diskId);
                    stateManager.startIdleStateWatchDog(diskId);

                    logger.debug("Spin up diskId:{} is successful.", diskId);
                } else {
                    stateManager.setState(diskId, DiskStateType.STANDBY);
                    logger.debug("Spin up diskId:{} is failed. and return state to STANDBY", diskId);
                }
            } finally {
                diskStateLocks[diskId].writeLock().unlock();
                logger.debug("Unlocked writeLock. disk state:{} diskId:{}", stateManager.getState(diskId), diskId);
            }
        } else {
            logger.debug("DiskId:{} is spinning now.", diskId);
        }

        try {} finally {
            if (diskStateLocks[diskId].getReadLockCount() > 0) {
                diskStateLocks[diskId].readLock().unlock();
                logger.debug("Unlocked readLock. disk state:{} diskId:{}", stateManager.getState(diskId), diskId);
            }
        }
    }

    public DiskStateType getState(int diskId) {
        return this.stateManager.getState(diskId);
    }

    public long getSleepingTimeByDiskId(int diskId) {
        return this.stateManager.getStandbyStartTime(diskId);
    }

    public int assignReplicaDiskId(int primaryDiskId, int replicaLevel) {
        return this.replicationPolicy.assignReplicationDiskId(
                primaryDiskId, replicaLevel);
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

    // TODO pull up
    private String getDiskFilePathPrefix(Block block) {
        int diskId = assignReplicaDiskId(
                block.getPrimaryDiskId(), block.getReplicaLevel());

        DiskFileAndDevicePath pathInfo = this.diskId2FilePath.get(diskId);

        return pathInfo.getDiskFilePath();
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


    ////////////---  These are tasks  ---////////////

    private class OperationTask implements Callable<Object> {

        private long blockId;
        private Block block;
        private IOType ioType;

        /**
         * this constructor be only used by write request.
         *
         * @param block
         * @param ioType
         */
        public OperationTask(Block block, IOType ioType) {
            this(block.getBlockId(), block, ioType);
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
                Callable readTask = new ReadPrimitiveTask(
                        blockId,
                        assignReplicaDiskId(block.getPrimaryDiskId(), block.getReplicaLevel()),
                        getDiskFilePathPrefix(block));

                int diskId = assignReplicaDiskId(
                        block.getPrimaryDiskId(),
                        block.getReplicaLevel());

                logger.debug("[Operation task] target DiskId:{}.  blockId:{} repLevel:{}",
                        diskId, block.getPrimaryDiskId(), block.getReplicaLevel());

                Future<byte[]> future = diskIOExecutors[diskId].submit(readTask);
                byte[] payload = future.get();

                result = new Block(
                        blockId,
                        0,
                        assignPrimaryDiskId(blockId),
                        0,
                        payload);
            } else if (ioType.equals(IOType.WRITE)) {
                Callable writeTask = new WritePrimitiveTask(
                        block,
                        getDiskFilePathPrefix(block));

                int diskId = assignReplicaDiskId(
                        block.getPrimaryDiskId(),
                        block.getReplicaLevel());

                Future<Block> future = diskIOExecutors[diskId].submit(writeTask);
                result = future.get();
            }

            return result;
        }
    }

    private class ReadPrimitiveTask implements Callable<byte[]> {

        private long blockId;
        private int diskId;
        private String diskFilePath;

        public ReadPrimitiveTask(long blockId, int diskId, String diskFilePath) {
            this.blockId = blockId;
            this.diskId = diskId;
            this.diskFilePath = diskFilePath;
        }

        @Override
        public byte[] call() throws Exception {

            logger.debug("[Read Primitive] start");

            byte[] result = null;

//            diskStateLocks[diskId].readLock().lock();
//            if (DiskStateType.STANDBY.equals(stateManager.getState(diskId)) ||
//                    DiskStateType.SPINDOWN.equals(stateManager.getState(diskId))) {
//
//                diskStateLocks[diskId].readLock().unlock();
//                diskStateLocks[diskId].writeLock().lock();
//
//                try {
//                    stateManager.setState(diskId, DiskStateType.SPINUP);
//                    if (!spinUp(diskId))
//                        throw new IllegalStateException(
//                                "Couldn't spin up the disk id: " + diskId);
//
//                    stateManager.setState(diskId, DiskStateType.IDLE);
//
//                } finally {
//                    diskStateLocks[diskId].writeLock().unlock();
//                }
//            }
//
//            try {} finally {
//                if (diskStateLocks[diskId].getReadLockCount() > 0)
//                    diskStateLocks[diskId].readLock().unlock();
//            }


            // when the disk is spinning then we can read the data from it.
            // and update the disk status IDLE to ACTIVE
            diskStateLocks[diskId].readLock().lock();

            logger.debug("Locked readLock. disk state:{} diskId:{}", stateManager.getState(diskId), diskId);

            if (DiskStateType.ACTIVE.equals(stateManager.getState(diskId)) ||
                    DiskStateType.IDLE.equals(stateManager.getState(diskId)) ||
                    DiskStateType.SPINUP.equals(stateManager.getState(diskId))) {

                diskStateLocks[diskId].readLock().unlock();
                logger.debug("Unlocked readLock. disk state:{} diskId:{}", stateManager.getState(diskId), diskId);
                diskStateLocks[diskId].writeLock().lock();
                logger.debug("Locked writeLock. disk state:{} diskId:{}", stateManager.getState(diskId), diskId);

                try {
                    File file = new File(this.diskFilePath + blockId);
                    if (!file.exists() || !file.isFile())
                        throw new IOException("[" + file.getCanonicalPath() + "] is not exist or not a file.");

                    result = new byte[(int)file.length()];

//                    BufferedInputStream bis =
//                            new BufferedInputStream(new FileInputStream(file));
//
//                    if (bis.available() < 1)
//                        throw new IOException("[" + this.diskFilePath + "] is not available.");

                    stateManager.setState(diskId, DiskStateType.ACTIVE);

//                    bis.read(result);
//                    bis.close();

                    // native read for avoid file system cache.
                    result = read(file.getCanonicalPath());

                    logger.info("Read a block from:{}. DataDiskId:{} Byte:{}",
                            file.getCanonicalPath(), diskId, file.length());

                    stateManager.setState(diskId, DiskStateType.IDLE);
                    stateManager.resetWatchDogTimer(diskId);
                    stateManager.startIdleStateWatchDog(diskId);

                } finally {
                    diskStateLocks[diskId].writeLock().unlock();
                    logger.debug("Unlocked writeLock. disk state:{} diskId:{}", stateManager.getState(diskId), diskId);
                }
            } else {
                logger.debug("Disk {} is not ACTIVE or IDLE or SPINUP state when to write to tha disk. It is [{}]", diskId, stateManager.getState(diskId));
            }

            try {} finally {
                if (diskStateLocks[diskId].getReadLockCount() > 0) {
                    diskStateLocks[diskId].readLock().unlock();
                    logger.debug("Unlocked readLock. disk state:{} diskId:{}", stateManager.getState(diskId), diskId);
                }
            }

            logger.debug("[Read Primitive] end");

            return result;
        }
    }

    private class WritePrimitiveTask implements Callable<Block> {

        private Block block;
        private String diskFilePath;

        public WritePrimitiveTask(Block block, String diskFilePath) {
            this.block = block;
            this.diskFilePath = diskFilePath;
        }

        @Override
        public Block call() throws Exception {

            logger.debug("[Write Primitive] start");

            Block result = null;

            int diskId = assignReplicaDiskId(
                    block.getPrimaryDiskId(),
                    block.getReplicaLevel());

//            diskStateLocks[diskId].readLock().lock();
//            if (DiskStateType.STANDBY.equals(stateManager.getState(diskId)) ||
//                    DiskStateType.SPINDOWN.equals(stateManager.getState(diskId))) {
//
//                diskStateLocks[diskId].readLock().unlock();
//                diskStateLocks[diskId].writeLock().lock();
//
//                try {
//                    stateManager.setState(diskId, DiskStateType.SPINUP);
//                    if (!spinUp(diskId))
//                        throw new IllegalStateException(
//                                "Couldn't spin up the disk id: " + diskId);
//
//                    stateManager.setState(diskId, DiskStateType.IDLE);
//
//                } finally {
//                    diskStateLocks[diskId].writeLock().unlock();
//                }
//            }
//
//            try {} finally {
//                if (diskStateLocks[diskId].getReadLockCount() > 0)
//                    diskStateLocks[diskId].readLock().unlock();
//            }


            diskStateLocks[diskId].readLock().lock();
            logger.debug("Locked readLock. disk state:{} diskId:{}", stateManager.getState(diskId), diskId);

            if (DiskStateType.ACTIVE.equals(stateManager.getState(diskId)) ||
                    DiskStateType.IDLE.equals(stateManager.getState(diskId)) ||
                    DiskStateType.SPINUP.equals(stateManager.getState(diskId))) {

                diskStateLocks[diskId].readLock().unlock();
                logger.debug("Unlocked readLock. disk state:{} diskId:{}", stateManager.getState(diskId), diskId);
                diskStateLocks[diskId].writeLock().lock();
                logger.debug("Locked writeLock. disk state:{} diskId:{}", stateManager.getState(diskId), diskId);

                try {
                    File file = new File(diskFilePath + block.getBlockId());
                    if (deleteOnExit) file.deleteOnExit();

                    checkDataDir(file.getParent());

                    if (!file.exists()) file.createNewFile();

//                    BufferedOutputStream bos =
//                            new BufferedOutputStream(new FileOutputStream(file));

                    stateManager.setState(diskId, DiskStateType.ACTIVE);
//                    bos.write(this.block.getPayload());
//                    bos.flush();
//                    bos.close();

                    // native write to avoid file system cache.
                    write(file.getCanonicalPath(), this.block.getPayload());

                    result = block;

                    logger.info("Written a block to:{}. DataDiskId:{} byte:{}",
                            file.getCanonicalPath(), diskId, this.block.getPayload().length);

                    stateManager.setState(diskId, DiskStateType.IDLE);
                    stateManager.resetWatchDogTimer(diskId);
                    stateManager.startIdleStateWatchDog(diskId);

                } finally {
                    diskStateLocks[diskId].writeLock().unlock();
                    logger.debug("Unlocked writeLock. disk state:{} diskId:{}", stateManager.getState(diskId), diskId);
                }
            } else {
                logger.debug("Disk {} is not ACTIVE or IDLE or SPINUP state when to write to tha disk. It is [{}]", diskId, stateManager.getState(diskId));
            }

            try {} finally {
                if (diskStateLocks[diskId].getReadLockCount() > 0) {
                    diskStateLocks[diskId].readLock().unlock();
                    logger.debug("Unlocked readLock. disk state:{} diskId:{}", stateManager.getState(diskId), diskId);
                }
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
