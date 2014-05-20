package jp.ac.titech.cs.de.ykstorage.storage.buffer.assignor;

public class DiskGroupAggregationAssignor implements IAssignor {

    private int numberOfBuffer;
    private int numberOfDisksPerGroup;

    public DiskGroupAggregationAssignor(int numberOfBuffer, int numberOfDisksPerGroup) {
        this.numberOfBuffer = numberOfBuffer;
        this.numberOfDisksPerGroup = numberOfDisksPerGroup;
    }

    @Override
    public int assign(long blockId, int primaryDiskId, int replicaLevel) {
        if (replicaLevel == 0) {
            return (int)Math.floor(primaryDiskId / numberOfDisksPerGroup) % numberOfBuffer;
        } else {
            return (assign(blockId, primaryDiskId, replicaLevel - 1) + 1) % numberOfBuffer;
        }
    }
}
