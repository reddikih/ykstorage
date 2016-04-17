package jp.ac.titech.cs.de.ykstorage.storage;

/**
 * Created by hikida on 2016/04/17.
 */
public class WriteReplicaResult {
    private final boolean result;
    private final Block block;

    WriteReplicaResult(boolean result, Block block) {
        this.result = result;
        this.block = block;
    }

    public boolean getResult() {
        return result;
    }

    public long getBlockId() {
        return block.getBlockId();
    }
}
