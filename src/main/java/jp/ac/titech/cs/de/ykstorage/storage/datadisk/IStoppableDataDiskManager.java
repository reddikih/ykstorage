package jp.ac.titech.cs.de.ykstorage.storage.datadisk;

import java.util.Collection;
import java.util.List;
import jp.ac.titech.cs.de.ykstorage.storage.Block;

public interface IStoppableDataDiskManager extends IDataDiskManager {

    public void spinUpDiskIfSleeping(int diskId);

    public List<Block> writeBlocks(Collection<Block> blocks);

}
