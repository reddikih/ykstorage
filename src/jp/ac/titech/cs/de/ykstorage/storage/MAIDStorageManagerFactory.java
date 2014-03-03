package jp.ac.titech.cs.de.ykstorage.storage;

import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.BufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.IBufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.CacheDiskManager;
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
        StateManager stateManager =
                new StateManager(
                        parameter.devicePathPrefix,
                        getCacheDiskDriveCharacters(),
                        parameter.spindownThresholdTime);

        CacheDiskManager cacheDiskManager =
                new CacheDiskManager(
                        parameter.cachediskCapacity,
                        parameter.BLOCK_SIZE,
                        parameter.numberOfCacheDisks,
                        parameter.diskFilePathPrefix,
                        parameter.devicePathPrefix,
                        getCacheDiskDriveCharacters(),
                        getPlacementPolicy(parameter.cacheDiskPlacementPolicy),
                        stateManager);

        cacheDiskManager.setDeleteOnExit(parameter.deleteOnExit);

        return cacheDiskManager;
    }

    @Override
    protected IDataDiskManager createDataDiskManager() {
        StateManager stateManager =
                new StateManager(
                        parameter.devicePathPrefix,
                        getDataDiskDriveCharacters(),
                        parameter.spindownThresholdTime);

        MAIDDataDiskManager dataDiskManager =
                new MAIDDataDiskManager(
                        parameter.numberOfDataDisks,
                        parameter.diskFilePathPrefix,
                        parameter.devicePathPrefix,
                        getDataDiskDriveCharacters(),
                        stateManager);
        dataDiskManager.setDeleteOnExit(parameter.deleteOnExit);

        return dataDiskManager;
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
