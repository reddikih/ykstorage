package jp.ac.titech.cs.de.ykstorage.storage;

import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.IBufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.ICacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.IDataDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.NormalDataDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.diskstate.StateManager;

import java.util.Arrays;


public class NormalStorageManagerFactory extends StorageManagerFactory {

    @Override
    protected IBufferManager createBufferManager() {
        return new IBufferManager() {
            @Override
            public Block read(long blockId) {
                return null;
            }

            @Override
            public Block write(Block block) {
                return null;
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

        return new NormalDataDiskManager(
                this.parameter.NUMBER_OF_DATA_DISKS,
                this.parameter.diskFilePathPrefix,
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
        return new NormalStorageManager(
                bufferManager,
                cacheDiskManager,
                dataDiskManager,
                this.parameter);
    }
}
