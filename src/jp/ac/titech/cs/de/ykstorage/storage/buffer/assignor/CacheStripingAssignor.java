package jp.ac.titech.cs.de.ykstorage.storage.buffer.assignor;

public class CacheStripingAssignor implements IAssignor {

    private int numberOfBuffer;

    public CacheStripingAssignor(int numberOfBuffer) {
        this.numberOfBuffer = numberOfBuffer;
    }

    @Override
    public int assign(long blockId, int primaryDiskId, int replicaLevel) {
        if (replicaLevel == 0) {
            return primaryDiskId % numberOfBuffer;
        } else {
            return (assign(blockId, primaryDiskId, replicaLevel - 1) + 1) % numberOfBuffer;
        }
    }
}
