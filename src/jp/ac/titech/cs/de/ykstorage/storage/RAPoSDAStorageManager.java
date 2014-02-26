package jp.ac.titech.cs.de.ykstorage.storage;


import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.IBufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.ICacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.IDataDiskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;

public class RAPoSDAStorageManager extends StorageManager {

    private final static Logger logger = LoggerFactory.getLogger(RAPoSDAStorageManager.class);

    private HashMap<Long, List<List<Long>>> key2blockIdMap = new HashMap<>();

    private int numberOfReplica;

    public RAPoSDAStorageManager(
            IBufferManager bufferManager,
            ICacheDiskManager cacheDiskManager,
            IDataDiskManager dataDiskManager,
            Parameter parameter) {
        super(bufferManager, cacheDiskManager, dataDiskManager, parameter);

    }

    public RAPoSDAStorageManager(
            IBufferManager bufferManager,
            ICacheDiskManager cacheDiskManager,
            IDataDiskManager dataDiskManager,
            Parameter parameter,
            int numberOfReplica) {
        this(bufferManager, cacheDiskManager, dataDiskManager, parameter);
        this.numberOfReplica = numberOfReplica;
    }


    @Override
    public byte[] read(long key) {
        return new byte[0];
    }

    @Override
    public boolean write(long key, byte[] value) {
        return false;
    }



    // TODO pull up method
    private List<List<Long>> getCorrespondingBlockIds(long key) {
        return this.key2blockIdMap.get(key);
    }


}
