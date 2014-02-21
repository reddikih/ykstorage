package jp.ac.titech.cs.de.ykstorage.storage.datadisk;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import jp.ac.titech.cs.de.ykstorage.storage.Block;
import jp.ac.titech.cs.de.ykstorage.storage.diskstate.StateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MAIDDataDiskManager implements IDataDiskManager {

    private final static Logger logger = LoggerFactory.getLogger(MAIDDataDiskManager.class);

    // TODO to be pull up field
    private boolean deleteOnExit = false;

    private boolean startedWatchdog = false;

    private String devicePrefix;
    private String diskFilePrefix;
    private int numberOfDataDisks;
    private Map<Integer, DiskFileAndDevicePath> diskId2FilePath;

    private ExecutorService[] diskIOExecutors;

    private final ExecutorService diskOperationExecutor = Executors.newCachedThreadPool();

    private StateManager stateManager;

    private ReadWriteLock[] diskStateLocks;

    public MAIDDataDiskManager(
            int numberOfDataDisks,
            String diskFilePrefix,
            String[] deviceCharacters,
            StateManager stateManager) {

        this.numberOfDataDisks = numberOfDataDisks;
        this.diskFilePrefix = diskFilePrefix;
        this.stateManager = stateManager;
        init(deviceCharacters);
    }

    private void init(String[] deviceCharacters) {

    }


    public List<Block> read(List<Long> blockIds) {
        return null;
    }

    @Override
    public boolean write(List<Block> blocks) {
        return false;
    }


    // TODO pull up
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
