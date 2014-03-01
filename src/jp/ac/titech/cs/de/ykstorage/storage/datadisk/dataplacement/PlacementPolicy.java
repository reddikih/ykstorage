package jp.ac.titech.cs.de.ykstorage.storage.datadisk.dataplacement;

public interface PlacementPolicy {

    public int assignDiskId(long blockId);

}
