package jp.ac.titech.cs.de.ykstorage.storage;

import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.IBufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.ICacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.IDataDiskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

public abstract class StorageManager {

    protected IBufferManager bufferManager;
    protected ICacheDiskManager cacheDiskManager;
    protected IDataDiskManager dataDiskManager;
    protected Parameter parameter;

    protected int blockSize;
    protected int numReplica;

    protected final AtomicLong sequenceNumber = new AtomicLong(0); // 0 origin

    public StorageManager(
            IBufferManager bufferManager,
            ICacheDiskManager cacheDiskManager,
            IDataDiskManager dataDiskManager,
            Parameter parameter) {
        this.bufferManager = bufferManager;
        this.cacheDiskManager = cacheDiskManager;
        this.dataDiskManager = dataDiskManager;
        this.parameter = parameter;
    }

    /**
     * A map between client request and corresponding blocks.
     * key; request key
     * value: a list of corresponding block ids
     */
    protected HashMap<Long, Long[]> requestMap;

    abstract public byte[] read(long key);

    abstract public boolean write(long key, byte[] value);

}
