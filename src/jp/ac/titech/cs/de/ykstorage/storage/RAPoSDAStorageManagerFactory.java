package jp.ac.titech.cs.de.ykstorage.storage;

import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.IBufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.ICacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.IDataDiskManager;

public class RAPoSDAStorageManagerFactory extends StorageManagerFactory {
    @Override
    protected IBufferManager createBufferManager() {
        return null;
    }

    @Override
    protected ICacheDiskManager createCacheDiskManager() {
        return null;
    }

    @Override
    protected IDataDiskManager createDataDiskManager() {
        return null;
    }

    @Override
    protected StorageManager createStorageManager(
            IBufferManager bufferManager,
            ICacheDiskManager cacheDiskManager,
            IDataDiskManager dataDiskManager,
            Parameter parameter) {
        return null;
    }
}
