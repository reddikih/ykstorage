package jp.ac.titech.cs.de.ykstorage.storage;

import java.util.Arrays;
import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.BufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.IBufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.ICacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.IDataDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.MAIDDataDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.diskstate.StateManager;


public class MAIDStorageManagerFactory extends StorageManagerFactory {

    @Override
    protected IBufferManager createBufferManager() {
        return new BufferManager(
                parameter.bufferCapacity,
                parameter.BLOCK_SIZE,
                parameter.bufferWaterMark);
    }

    @Override
    protected ICacheDiskManager createCacheDiskManager() {
        return new ICacheDiskManager() {
            @Override
            public Block read(Long blockId) {
                return null;
            }

            @Override
            public Block write(Block blocks) {
                return null;
            }
        };
    }

    @Override
    protected IDataDiskManager createDataDiskManager() {
        StateManager stateManager =
                new StateManager(
                        parameter.devicePathPrefix,
                        parameter.driveCharacters,
                        parameter.spindownThresholdTime);

        String[] dataDiskDriveChars = Arrays.copyOfRange(
                parameter.driveCharacters,
                parameter.numberOfCacheDisks == 0 ? 0 : parameter.numberOfCacheDisks - 1,
                parameter.driveCharacters.length);

        return new MAIDDataDiskManager(
                parameter.numberOfDataDisks,
                parameter.diskFilePathPrefix,
                parameter.devicePathPrefix,
                dataDiskDriveChars,
                stateManager);
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
