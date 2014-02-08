package jp.ac.titech.cs.de.ykstorage.storage.cachedisk;

import jp.ac.titech.cs.de.ykstorage.storage.Block;

public interface ICacheDiskManager {

    public Block read(Block block);

    public boolean write(Block blocks);
}
