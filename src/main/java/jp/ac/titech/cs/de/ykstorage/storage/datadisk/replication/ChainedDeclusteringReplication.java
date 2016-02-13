package jp.ac.titech.cs.de.ykstorage.storage.datadisk.replication;

public class ChainedDeclusteringReplication implements ReplicationPolicy {

    private final int numberOfDisks;

    public ChainedDeclusteringReplication(int numberOfDisks) {
        this.numberOfDisks = numberOfDisks;
    }

    @Override
    public int assignReplicationDiskId(int primaryDiskId, int replicaLevel) {
        return (primaryDiskId + replicaLevel) % this.numberOfDisks;
    }
}
