package jp.ac.titech.cs.de.ykstorage.storage;

import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.IBufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.RAPoSDABufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.CacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.ICacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.IDataDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.RAPoSDADataDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.diskstate.StateManager;

public class RAPoSDAStorageManagerFactory extends StorageManagerFactory {

    @Override
    protected IBufferManager createBufferManager() {

        RAPoSDABufferManager bufferManager = new RAPoSDABufferManager(
                parameter.numberOfBuffers,
                parameter.bufferCapacity,
                parameter.BLOCK_SIZE,
                parameter.bufferWaterMark,
                parameter.numberOfReplicas,
                getBufferAssignor(parameter.bufferAssignor));

        return bufferManager;
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

        RAPoSDADataDiskManager dataDiskManager = new RAPoSDADataDiskManager(
                parameter.numberOfDataDisks,
                parameter.diskFilePathPrefix,
                parameter.devicePathPrefix,
                getDataDiskDriveCharacters(),
                getPlacementPolicy(parameter.dataDiskPlacementPolicy),
                getReplicationPolicy(parameter.dataDiskReplicationPolicy),
                stateManager
        );

        dataDiskManager.setDeleteOnExit(parameter.deleteOnExit);

        return dataDiskManager;
    }

    @Override
    protected StorageManager createStorageManager(
            IBufferManager bufferManager,
            ICacheDiskManager cacheDiskManager,
            IDataDiskManager dataDiskManager,
            Parameter parameter) {

        return new RAPoSDAStorageManager(
                bufferManager,
                cacheDiskManager,
                dataDiskManager,
                parameter,
                parameter.numberOfReplicas);
    }
}
