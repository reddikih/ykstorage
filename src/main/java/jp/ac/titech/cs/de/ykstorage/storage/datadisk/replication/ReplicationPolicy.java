package jp.ac.titech.cs.de.ykstorage.storage.datadisk.replication;

public interface ReplicationPolicy {

    public int assignReplicationDiskId(int primaryDiskId, int replicaLevel);

}
