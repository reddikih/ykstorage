package jp.ac.titech.cs.de.ykstorage.storage.datadisk.dataplacement;

public class RoundRobinPlacement implements PlacementPolicy {

    private final int numberOfDisks;

    public RoundRobinPlacement(int numberOfDisks) {
        this.numberOfDisks = numberOfDisks;
    }

    @Override
    public int assignDiskId(long blockId) {
        return (int)(blockId % numberOfDisks);
    }

}
