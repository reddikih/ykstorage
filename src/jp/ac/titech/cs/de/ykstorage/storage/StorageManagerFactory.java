package jp.ac.titech.cs.de.ykstorage.storage;

import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.IBufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.ICacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.IDataDiskManager;

public abstract class StorageManagerFactory {

    protected Parameter parameter;

    public void setParameter(Parameter parameter) {this.parameter = parameter;}

    public static StorageManagerFactory getStorageManagerFactory(Parameter parameter) {
        StorageManagerFactory factory = null;
        try {
            factory = (StorageManagerFactory)Class.forName(
                    StorageManagerFactory.class.getPackage().getName() + "." +
                    parameter.storageManagerFactory).newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        factory.setParameter(parameter);
        return factory;
    }

    public StorageManager createStorageManager() {
        IBufferManager bufferManager = createBufferManager();
        ICacheDiskManager cacheDiskManager = createCacheDiskManager();
        IDataDiskManager dataDiskManager = createDataDiskManager();

        StorageManager storageManager =
                createStorageManager(bufferManager, cacheDiskManager, dataDiskManager, parameter);
        return storageManager;
    }

    protected abstract IBufferManager createBufferManager();
    protected abstract ICacheDiskManager createCacheDiskManager();
    protected abstract IDataDiskManager createDataDiskManager();

    protected abstract StorageManager createStorageManager(
            IBufferManager bufferManager,
            ICacheDiskManager cacheDiskManager,
            IDataDiskManager dataDiskManager,
            Parameter parameter
    );
}
