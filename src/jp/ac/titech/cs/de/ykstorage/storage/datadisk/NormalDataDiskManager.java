package jp.ac.titech.cs.de.ykstorage.storage.datadisk;

import jp.ac.titech.cs.de.ykstorage.storage.Block;
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

public class NormalDataDiskManager implements IDataDiskManager {

    private final static Logger logger = LoggerFactory.getLogger(NormalDataDiskManager.class);

    private boolean deleteOnExit = false;

    private String devicePrefix = "/dev/sd";
    private String diskFilePrefix;
    private int numberOfDataDisks;
    private Map<Integer, DiskFileAndDevicePath> diskId2FilePath;

    private ExecutorService[] diskIOExecutors;
    private ExecutorService diskOperationExecutor;

    public NormalDataDiskManager(int numberOfDataDisks, String diskFilePrefix, char[] deviceCharacters) {
        this.diskFilePrefix = diskFilePrefix;
        this.numberOfDataDisks = numberOfDataDisks;

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

        this.diskOperationExecutor = Executors.newCachedThreadPool();

        this.diskIOExecutors = new ExecutorService[this.numberOfDataDisks];
        for (int i=0; i < numberOfDataDisks; i++) {
            diskIOExecutors[i] = Executors.newFixedThreadPool(1);
        }
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
        int diskId = assginPrimaryDiskId(blockId);
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
                ReadPrimitiveTask readTask = new ReadPrimitiveTask(blockId, getDiskFilePathPrefix(blockId));

                Future<byte[]> future = diskIOExecutors[assginPrimaryDiskId(blockId)].submit(readTask);
                byte[] payload = future.get();

                result = new Block(blockId, 0, assginPrimaryDiskId(blockId), 0, payload);
            } else if (ioType.equals(IOType.WRITE)) {
                WritePrimitiveTask writeTask = new WritePrimitiveTask(block, getDiskFilePathPrefix(block.getBlockId()));

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
    public int assginPrimaryDiskId(long blockId) {
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
