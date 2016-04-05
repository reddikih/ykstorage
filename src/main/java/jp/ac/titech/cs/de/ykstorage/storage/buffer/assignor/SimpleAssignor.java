package jp.ac.titech.cs.de.ykstorage.storage.buffer.assignor;

public class SimpleAssignor implements IAssignor {

    private final int numberOfBuffer;

    public SimpleAssignor(int numberOfBuffer) {
        if (numberOfBuffer == 0)
            throw new IllegalArgumentException("invalid number of buffer: " + numberOfBuffer);
        this.numberOfBuffer = numberOfBuffer;
    }

    @Override
    public int assign(long blockId, int primaryDiskId, int replicaLevel) {
        return (int)blockId % numberOfBuffer;
    }
}
