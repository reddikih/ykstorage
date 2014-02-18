package jp.ac.titech.cs.de.ykstorage.storage.datadisk;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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

public class NormalDataDiskManager implements IDataDiskManager, IdleThresholdListener {

    private final static Logger logger = LoggerFactory.getLogger(NormalDataDiskManager.class);

    private boolean deleteOnExit = false;

    private String devicePrefix = "/dev/sd";
    private String diskFilePrefix;
    private int numberOfDataDisks;
    private Map<Integer, DiskFileAndDevicePath> diskId2FilePath;

    private ExecutorService[] diskIOExecutors;
    private final ExecutorService diskOperationExecutor = Executors.newCachedThreadPool();

    private StateManager stateManager;

    private ReadWriteLock[] diskStateLocks;

    public NormalDataDiskManager(
            int numberOfDataDisks,
            String diskFilePrefix,
            char[] deviceCharacters) {

        this(numberOfDataDisks, diskFilePrefix, deviceCharacters, null);
    }

    public NormalDataDiskManager(
            int numberOfDataDisks,
            String diskFilePrefix,
            char[] deviceCharacters,
            StateManager stateManager) {

        this.diskFilePrefix = diskFilePrefix;
        this.numberOfDataDisks = numberOfDataDisks;
        this.stateManager = stateManager;
        init(deviceCharacters);
    }

    private void init(char[] deviceCharacters) {
        this.diskId2FilePath = new HashMap<>();

        int diskId= 0;
        for (char deviceChar : deviceCharacters) {
            DiskFileAndDevicePath pathInfo = new DiskFileAndDevicePath(
                    this.diskFilePrefix + deviceChar + "/", this.devicePrefix + deviceChar);
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

    private String getDiskFilePathPrefix(long blockId) {
        int diskId = assignPrimaryDiskId(blockId);
        DiskFileAndDevicePath pathInfo = this.diskId2FilePath.get(diskId);
        return pathInfo.getDiskFilePath();
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
                    logger.debug("Spinning down diskId: {} is successful.", diskId);
                } else {
                    stateManager.setState(diskId, DiskStateType.IDLE);
                    logger.debug("Spinning down diskId: {} is failed. and return state to IDLE", diskId);
                }
            } finally {
                diskStateLocks[diskId].writeLock().unlock();
            }
        }
    }

    private boolean spinDown(int diskId) {
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
        return true;
    }

    private boolean spinUp(int diskId) {
        String devicePath = this.diskId2FilePath.get(diskId).getDevicePath();
        if (!devicePathCheck(devicePath)) return false;

        String command = "ls " + devicePath;
        int rc = executeExternalCommand(command);
        logger.debug("return value of [{}]: {}", command, rc);
        if (rc != 0) return false;
        // TODO increment spin up count.
        // and the other some operation if needed.
        return true;
    }

    private int executeExternalCommand(String command) {
        int returnCode = 1;

        try {
            Process process = Runtime.getRuntime().exec(command);
            returnCode = process.waitFor();
        } catch (IOException e) {
            launderThrowable(e);
        } catch (InterruptedException e) {
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
            byte[] result;

            File file = new File(this.diskFilePath + blockId);
            if (!file.exists() || !file.isFile())
                throw new IOException("[" + file.getCanonicalPath() + "] is not exist or not a file.");

            result = new byte[(int)file.length()];

            BufferedInputStream bis = null;
            bis = new BufferedInputStream(new FileInputStream(file));
            if (bis.available() < 1)
                throw new IOException("[" + this.diskFilePath + "] is not available.");

            bis.read(result);
            bis.close();

            return result;
        }
    }

    private class ReadPrimitiveTaskWithStateManagement implements Callable<byte[]> {

        private long blockId;
        private String diskFilePath;

        public ReadPrimitiveTaskWithStateManagement(long blockId, String diskFilePath) {
            this.blockId = blockId;
            this.diskFilePath = diskFilePath;
        }

        @Override
        public byte[] call() throws Exception {
            byte[] result = null;

            // check disk state either the disk is spinning or not.
            int diskId = assignPrimaryDiskId(blockId);
            diskStateLocks[diskId].readLock().lock();
            if (DiskStateType.STANDBY.equals(stateManager.getState(diskId)) ||
                    DiskStateType.SPINDOWN.equals(stateManager.getState(diskId))) {
                diskStateLocks[diskId].readLock().unlock();
                diskStateLocks[diskId].writeLock().lock();
                try {
                    // re-check state because another thread might have acquired
                    // write lock and changed state before we did.
                    if (DiskStateType.STANDBY.equals(stateManager.getState(diskId))) {
                        stateManager.setState(diskId, DiskStateType.SPINUP);
                        diskStateLocks[diskId].readLock().lock();

                    }
                } finally {
                    // unlock write still folding read lock
                    diskStateLocks[diskId].writeLock().unlock();
                }

                try {
                    boolean isSuccess = spinUp(diskId);
                    logger.debug("spinning up disk id:{} is {}", diskId, isSuccess);
                } finally {
                    diskStateLocks[diskId].readLock().unlock();
                }
            }

            // when the disk is spinning then we can read the data from it.
            // and update the disk status IDLE to ACTIVE
            diskStateLocks[diskId].readLock().lock();
            if (DiskStateType.ACTIVE.equals(stateManager.getState(diskId)) ||
                    DiskStateType.IDLE.equals(stateManager.getState(diskId))) {

                diskStateLocks[diskId].readLock().unlock();
                diskStateLocks[diskId].writeLock().lock();

                try {
                    if (DiskStateType.ACTIVE.equals(stateManager.getState(diskId)) ||
                            DiskStateType.IDLE.equals(stateManager.getState(diskId))) {
                        stateManager.setState(diskId, DiskStateType.ACTIVE);
                        diskStateLocks[diskId].readLock().lock();
                    }
                } finally {
                    // unlock write still folding read lock
                    diskStateLocks[diskId].writeLock().unlock();
                }

                try {
                    // TODO 停止中ディスクのファイル情報もディスクを停止したまま取得できるか要確認
                    File file = new File(this.diskFilePath + blockId);
                    if (!file.exists() || !file.isFile())
                        throw new IOException("[" + file.getCanonicalPath() + "] is not exist or not a file.");

                    result = new byte[(int)file.length()];

                    BufferedInputStream bis = null;
                    bis = new BufferedInputStream(new FileInputStream(file));
                    if (bis.available() < 1)
                        throw new IOException("[" + this.diskFilePath + "] is not available.");

                    bis.read(result);
                    bis.close();
                } finally {
                    diskStateLocks[diskId].readLock().unlock();
                }
            }

            // when read is finished then we change the disk status from ACTIVE to IDLE
            // and invoke idle time counter.
            diskStateLocks[diskId].readLock().lock();
            if (DiskStateType.ACTIVE.equals(stateManager.getState(diskId))) {
                diskStateLocks[diskId].readLock().unlock();
                diskStateLocks[diskId].writeLock().lock();
                try {
                    stateManager.setState(diskId, DiskStateType.IDLE);
                    stateManager.resetWatchDogTimer(diskId);

                    diskStateLocks[diskId].readLock().lock();
                } finally {
                    diskStateLocks[diskId].writeLock().unlock();
                    diskStateLocks[diskId].readLock().lock();
                }
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
            boolean result;

            File file = new File(diskFilePath + block.getBlockId());
            if (deleteOnExit) file.deleteOnExit();

            checkDataDir(file.getParent());

            logger.info("write to: {}", file.getCanonicalPath());

            if (!file.exists()) file.createNewFile();

            BufferedOutputStream bos = null;
            bos = new BufferedOutputStream(new FileOutputStream(file));

            bos.write(this.block.getPayload());
            bos.flush();
            bos.close();

            result = true;

            logger.info("written successfully. to: {}, {}[byte]",
                    file.getCanonicalPath(), this.block.getPayload().length);

            return result;
        }
    }

    private class WritePrimitiveTaskWithStateManagement implements Callable<Boolean> {

        private Block block;
        private String diskFilePath;

        public WritePrimitiveTaskWithStateManagement(Block block, String diskFilePath) {
            this.block = block;
            this.diskFilePath = diskFilePath;
        }

        @Override
        public Boolean call() throws Exception {
            boolean result;

            int diskId = block.getPrimaryDiskId();
            diskStateLocks[diskId].readLock().lock();
            if (DiskStateType.STANDBY.equals(stateManager.getState(diskId))) {
                diskStateLocks[diskId].readLock().unlock();
                diskStateLocks[diskId].writeLock().lock();

            }

            return null;
        }
    }


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

    private enum IOType {
        READ,
        WRITE,
    }

    public void checkDataDir(String dir) throws IOException {
        File file = new File(dir);
        if (deleteOnExit) file.deleteOnExit();

        if (!file.exists()) {
            if (!file.mkdirs())
                logger.info("could not create dir: {}", file.getCanonicalPath());
        }
    }

    @Override
    public int assignPrimaryDiskId(long blockId) {
        BigInteger numerator = BigInteger.valueOf(blockId);
        BigInteger denominator = BigInteger.valueOf(this.numberOfDataDisks);
        return numerator.mod(denominator).intValue();
    }

    private RuntimeException launderThrowable(Throwable t) {
        if (t instanceof RuntimeException) return (RuntimeException) t;
        else if (t instanceof Error) throw (Error) t;
        else throw new IllegalStateException("Not unchecked", t);
    }

    public void setDeleteOnExit(boolean deleteOnExit) {
        this.deleteOnExit = deleteOnExit;
    }

}
