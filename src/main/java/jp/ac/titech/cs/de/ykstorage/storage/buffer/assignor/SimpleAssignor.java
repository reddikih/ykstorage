package jp.ac.titech.cs.de.ykstorage.storage.buffer.assignor;

public class SimpleAssignor implements IAssignor {

    private int numberOfBuffer;

    public SimpleAssignor(int numberOfBuffer) {
        this.numberOfBuffer = numberOfBuffer;
    }

    @Override
    public int assign(long blockId, int primaryDiskId, int replicaLevel) {
        return (int)blockId % numberOfBuffer;
    }
}
