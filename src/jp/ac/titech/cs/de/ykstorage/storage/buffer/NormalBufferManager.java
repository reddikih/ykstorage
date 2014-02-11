package jp.ac.titech.cs.de.ykstorage.storage.buffer;

import jp.ac.titech.cs.de.ykstorage.storage.Block;

public class NormalBufferManager implements IBufferManager {

    private Buffer buffer = new DummyBuffer();

    @Override
    public Block read(long blockId) {
        return null;
    }

    @Override
    public boolean write(Block block) {
        return false;
    }

    @Override
    public Block remove(long blockId) {
        return null;
    }
}
