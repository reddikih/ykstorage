package jp.ac.titech.cs.de.ykstorage.storage.buffer;

import jp.ac.titech.cs.de.ykstorage.storage.Block;

public interface ReplacePolicy {

    public Block add(Block key);
}
