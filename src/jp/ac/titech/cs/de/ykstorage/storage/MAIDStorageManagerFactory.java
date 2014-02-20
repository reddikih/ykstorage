package jp.ac.titech.cs.de.ykstorage.storage;

import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.IBufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.ICacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.IDataDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.MAIDDataDiskManager;


public class MAIDStorageManagerFactory extends StorageManagerFactory {

    @Override
    protected IBufferManager createBufferManager() {
        return new IBufferManager() {
            @Override
            public Block read(long blockId) {
                return null;
            }

            @Override
            public boolean write(Block block) {
                return false;
            }

            @Override
            public Block remove(long blockId) {
                return null;
            }
        };
    }

    @Override
    protected ICacheDiskManager createCacheDiskManager() {
        return new ICacheDiskManager() {
            @Override
            public Block read(Long blockId) {
                return null;
            }

            @Override
            public boolean write(Block blocks) {
                return false;
            }
        };
    }

    @Override
    protected IDataDiskManager createDataDiskManager() {
        return new MAIDDataDiskManager();
    }

    @Override
    protected StorageManager createStorageManager(
            IBufferManager bufferManager,
            ICacheDiskManager cacheDiskManager,
            IDataDiskManager dataDiskManager,
            Parameter parameter) {
        return new MAIDStorageManager(
                bufferManager,
                cacheDiskManager,
                dataDiskManager,
                parameter);
    }
}
