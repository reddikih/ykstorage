package jp.ac.titech.cs.de.ykstorage.storage.buffer;

import jp.ac.titech.cs.de.ykstorage.storage.Block;

public interface IBufferManager {

    @Deprecated
    public Block read(long blockId);

    public Block read(Block block);

    @Deprecated
    public Block remove(long blockId);

    public Block remove(Block block);

    public Block write(Block block);
}
