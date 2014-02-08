package jp.ac.titech.cs.de.ykstorage.storage.buffer;

import jp.ac.titech.cs.de.ykstorage.storage.Block;

public interface IBufferManager {
    public byte[] read(long key);

    public boolean write(long key, Block block);

    public byte[] remove(long key);
}
