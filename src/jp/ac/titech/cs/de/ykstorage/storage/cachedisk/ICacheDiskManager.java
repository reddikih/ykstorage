package jp.ac.titech.cs.de.ykstorage.storage.cachedisk;

import jp.ac.titech.cs.de.ykstorage.storage.Block;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public interface ICacheDiskManager {

    public Block read(Long blockId);

    public Block write(Block blocks) throws IOException, ExecutionException, InterruptedException;
}
