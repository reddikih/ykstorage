package jp.ac.titech.cs.de.ykstorage.storage.cachedisk;

import jp.ac.titech.cs.de.ykstorage.storage.Block;

public interface ICacheDiskManager {

    public Block read(Long blockId);

    public Block write(Block blocks);
}
