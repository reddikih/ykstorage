package jp.ac.titech.cs.de.ykstorage.storage;

import java.util.List;
import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.IBufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.ICacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.IDataDiskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MAIDStorageManager extends StorageManager {

    private final static Logger logger = LoggerFactory.getLogger(MAIDStorageManager.class);

    public MAIDStorageManager(
            IBufferManager bufferManager,
            ICacheDiskManager cacheDiskManager,
            IDataDiskManager dataDiskManager,
            Parameter parameter) {
        super(bufferManager, cacheDiskManager, dataDiskManager, parameter);
    }

    @Override
    public byte[] read(long key) {
        List<Long> blockIds = getCorrespondingBlockIds(key);
        return new byte[0];
    }

    @Override
    public boolean write(long key, byte[] value) {
        return false;
    }

    private List<Long> getCorrespondingBlockIds(long key) {
        return this.key2blockIdMap.get(key);
    }
}
