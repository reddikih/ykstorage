package jp.ac.titech.cs.de.ykstorage.storage;

import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.IBufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.ICacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.IDataDiskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class DumStorageManager extends StorageManager {

    private final static Logger logger = LoggerFactory.getLogger(DumStorageManager.class);

    public DumStorageManager(
            IBufferManager bufferManager,
            ICacheDiskManager cacheDiskManager,
            IDataDiskManager dataDiskManager,
            Parameter parameter) {
        super(bufferManager, cacheDiskManager, dataDiskManager, parameter);
    }

    @Override
    public byte[] read(long key) {
        byte[] result;
        BufferedInputStream bis = null;
        File file = new File(Parameter.DATA_DIR + "/test_" + key);
        if (!file.exists() && !file.isFile())
            throw new RuntimeException("read file is not exist or is not a file");
        result = new byte[(int)file.length()];

        try {
            bis = new BufferedInputStream(new FileInputStream(file));
            if (bis.available() < 1)
                throw new IOException("read file is not available.");
            bis.read(result);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (bis != null) {
                try {
                    bis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    result = null;
                }
            }
        }
        return result;
    }

    @Override
    public boolean write(long key, byte[] value) {
        boolean result = false;
        BufferedOutputStream bos = null;
        try {
            checkDataDir();
            File file = new File(Parameter.DATA_DIR + "/test_" + key);
            logger.debug("write to:[{}]", file.getCanonicalPath());
            if (!file.exists()) file.createNewFile();
            bos = new BufferedOutputStream((new FileOutputStream(file)));
            bos.write(value);
            bos.flush();
            result = true;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (bos != null) try {
                bos.close();
            } catch (IOException e) {
                e.printStackTrace();
                result = false;
            }
        }
        return result;
    }

    public void checkDataDir() {
        File file = new File(Parameter.DATA_DIR);
        if (!file.exists()) {
            file.mkdir();
        }
    }
}
