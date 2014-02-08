package jp.ac.titech.cs.de.ykstorage.storage;

import jp.ac.titech.cs.de.ykstorage.storage.buffer.CacheMemoryManager;

public class NormalStorageManager extends OLDStorageManager {

    public NormalStorageManager(CacheMemoryManager cmm, DiskManager dm) {
        super(cmm, dm);
    }
}
