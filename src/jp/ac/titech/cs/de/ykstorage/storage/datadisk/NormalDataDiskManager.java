package jp.ac.titech.cs.de.ykstorage.storage.datadisk;

import jp.ac.titech.cs.de.ykstorage.storage.Block;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class NormalDataDiskManager implements IDataDiskManager {

    private final static Logger logger = LoggerFactory.getLogger(NormalDataDiskManager.class);

    private String devicePrefix = "/dev/sd";
    private String diskFilePrefix;
    private int numberOfDataDisks;
    private Map<Integer, DiskFileAndDevicePath> diskId2FilePath;

    private ExecutorService[] diskIOExecutors;
    private ExecutorService diskOperationExecutor;

    public NormalDataDiskManager(int numberOfDataDisks, String diskFilePrefix, String[] deviceCharacters) {
        this.diskFilePrefix = diskFilePrefix;
        this.numberOfDataDisks = numberOfDataDisks;
        init(deviceCharacters);
    }

    private void init(String[] deviceCharacters) {
        this.diskId2FilePath = new HashMap<Integer, DiskFileAndDevicePath>();

        int diskId= 0;
        for (String deviceChar : deviceCharacters) {
            DiskFileAndDevicePath pathInfo = new DiskFileAndDevicePath(
                    this.diskFilePrefix + deviceChar, this.devicePrefix + deviceChar);
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
        return null;
    }

    @Override
    public boolean write(List<Block> blocks) {
        List<OperationTask> operations = new ArrayList<OperationTask>();
        for (Block block : blocks)
            operations.add(new OperationTask(block, IOType.WRITE));

        try {
            List<Future<Object>> futures = this.diskOperationExecutor.invokeAll(operations);

        } catch (InterruptedException e) {
            throw launderThrowable(e);
        }

        return false;
    }

    private String getDiskPath(Block block) {
        int diskId = block.getPrimaryDiskId();
        DiskFileAndDevicePath pathInfo = this.diskId2FilePath.get(diskId);
        return pathInfo.getDiskFilePath();
    }

    private class OperationTask implements Callable<Object> {

        private long blockId;
        private Block block;
        private IOType ioType;

        public OperationTask(long blockId, IOType ioType) {
            this(blockId, null, ioType);
        }

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
                ReadPrimitiveTask readTask = new ReadPrimitiveTask(getDiskPath(block));
                Future<byte[]> future = diskIOExecutors[block.getPrimaryDiskId()].submit(readTask);
                // TODO BlockクラスをBlockInfoに変更？
//                result = new Block
            } else if (ioType.equals(IOType.WRITE)) {
                WritePrimitiveTask writeTask = new WritePrimitiveTask(block, getDiskPath(block));
                Future<Boolean> future = diskIOExecutors[block.getPrimaryDiskId()].submit(writeTask);
                boolean isWritten = future.get();
                if (isWritten) {
                    result = block;
                }
            }
            return result;
        }
    }

    // TODO implement read io task
    private class ReadPrimitiveTask implements Callable<byte[]> {

        private String diskFilePath;

        public ReadPrimitiveTask(String diskFilePath) {
            this.diskFilePath = diskFilePath;
        }

        @Override
        public byte[] call() throws Exception {
            byte[] result;

            File file = new File(this.diskFilePath);
            if (!file.exists() || !file.isFile())
                throw new IOException("[" + this.diskFilePath + "] is not exist or not a file.");

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

    // TODO implement write io task
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
            File file = new File(diskFilePath);
            checkDataDir(file.getParent());

            logger.info("write to: [{}]", file.getCanonicalPath());

            if (!file.exists()) file.createNewFile();

            BufferedOutputStream bos = null;
            bos = new BufferedOutputStream(new FileOutputStream(file));

            bos.write(this.block.getPayload());
            bos.flush();
            bos.close();

            result = true;

            logger.info("written successfully. to: [{}], {}[byte]",
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

    public void checkDataDir(String dir) {
        File file = new File(dir);
        if (!file.exists()) {
            file.mkdir();
        }
    }

    private RuntimeException launderThrowable(Throwable t) {
        if (t instanceof RuntimeException) return (RuntimeException) t;
        else if (t instanceof Error) throw (Error) t;
        else throw new IllegalStateException("Not unchecked", t);
    }
}
