package jp.ac.titech.cs.de.ykstorage.storage.buffer;

import jp.ac.titech.cs.de.ykstorage.storage.Block;

public class NormalBufferManager implements IBufferManager {

    @Override
    public byte[] read(long key) {
        return new byte[0];
    }

    @Override
    public boolean write(long key, Block block) {
        return false;
    }

    @Override
    public byte[] remove(long key) {
        return new byte[0];
    }
}
