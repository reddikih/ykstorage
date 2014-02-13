package jp.ac.titech.cs.de.ykstorage.storage;

import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.IBufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.ICacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.IDataDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.NormalDataDiskManager;


public class NormalStorageManagerFactory extends StorageManagerFactory {

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
        return new NormalDataDiskManager(
                this.parameter.NUMBER_OF_DATA_DISKS,
                this.parameter.diskFilePathPrefix,
                this.parameter.driveCharacters);
    }

    @Override
    protected StorageManager createStorageManager(
            IBufferManager bufferManager,
            ICacheDiskManager cacheDiskManager,
            IDataDiskManager dataDiskManager,
            Parameter parameter) {
        return new NormalStorageManager(
                bufferManager,
                cacheDiskManager,
                dataDiskManager,
                this.parameter);
    }
}
