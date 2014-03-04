package jp.ac.titech.cs.de.ykstorage.storage;

import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.IBufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.assignor.IAssignor;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.ICacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.IDataDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.dataplacement.PlacementPolicy;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.replication.ReplicationPolicy;

import java.lang.reflect.Constructor;
import java.util.Arrays;

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

    protected String[] getCacheDiskDriveCharacters() {
        return Arrays.copyOfRange(
                parameter.driveCharacters,
                0,
                parameter.numberOfCacheDisks);
    }

    protected String[] getDataDiskDriveCharacters() {
        return Arrays.copyOfRange(
                parameter.driveCharacters,
                parameter.numberOfCacheDisks,
                parameter.numberOfCacheDisks + parameter.numberOfDataDisks);
    }

    protected PlacementPolicy getPlacementPolicy(String placementPolicyName, int numberOfDisks) {
        PlacementPolicy placementPolicy = null;

        try {
            Class clazz = Class.forName(
                    PlacementPolicy.class.getPackage().getName() + "." +
                    placementPolicyName + "Placement");
            Constructor constructor = clazz.getConstructor(int.class);
            placementPolicy = (PlacementPolicy)constructor.newInstance(numberOfDisks);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        return placementPolicy;
    }

    protected ReplicationPolicy getReplicationPolicy(String replicationPolicyName) {
        ReplicationPolicy replicationPolicy = null;

        try {
            Class clazz = Class.forName(
                    ReplicationPolicy.class.getPackage().getName() + "." +
                    replicationPolicyName + "Replication"
            );
            Constructor constructor = clazz.getConstructor(int.class);
            replicationPolicy = (ReplicationPolicy)constructor.newInstance(parameter.numberOfDataDisks);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        return replicationPolicy;
    }

    protected IAssignor getBufferAssignor(String assignorName) {
        IAssignor assignor = null;

        try {
            Class clazz = Class.forName(
                    IAssignor.class.getPackage().getName() + "." + assignorName + "Assignor");
            Constructor constructor = clazz.getConstructor(int.class);
            assignor = (IAssignor)constructor.newInstance(parameter.numberOfBuffers);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        return assignor;
    }
}
