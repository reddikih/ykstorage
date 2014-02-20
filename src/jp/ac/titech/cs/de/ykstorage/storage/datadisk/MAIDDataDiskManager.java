package jp.ac.titech.cs.de.ykstorage.storage.datadisk;

import java.util.List;
import jp.ac.titech.cs.de.ykstorage.storage.Block;

public class MAIDDataDiskManager implements IDataDiskManager {

    @Override
    public List<Block> read(List<Long> blockIds) {
        return null;
    }

    @Override
    public boolean write(List<Block> blocks) {
        return false;
    }

    @Override
    public int assignPrimaryDiskId(long blockId) {
        return 0;
    }
}
