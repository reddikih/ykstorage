package jp.ac.titech.cs.de.ykstorage.storage.buffer;

import jp.ac.titech.cs.de.ykstorage.storage.Block;

public interface IBufferManager {

    public Block read(long blockId);

    public boolean write(Block block);

    public Block remove(long blockId);
}
