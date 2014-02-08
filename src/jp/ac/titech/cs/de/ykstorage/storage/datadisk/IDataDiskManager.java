package jp.ac.titech.cs.de.ykstorage.storage.datadisk;

import jp.ac.titech.cs.de.ykstorage.storage.Block;

import java.util.List;

public interface IDataDiskManager {

    public byte[] read(List<Long> blockIds);

    public boolean write(List<Block> blocks);

}
