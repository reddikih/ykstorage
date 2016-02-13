import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import jp.ac.titech.cs.de.ykstorage.frontend.FrontEndTest;
import jp.ac.titech.cs.de.ykstorage.storage.BlockTest;
import jp.ac.titech.cs.de.ykstorage.storage.MAIDStorageManagerTest;
import jp.ac.titech.cs.de.ykstorage.storage.RAPoSDAStorageManagerTest;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.BufferManagerTest;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.BufferRegionTest;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.LRUBufferTest;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.RAPoSDABufferManagerTest;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.MAIDDataDiskManagerTest;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.NormalDataDiskManagerTest;

/**
 * Note:
 *     You should run the MAIDStorageManagerTest independently.
 *     It need to override some final configuration of Parameter
 *     to test.
 */

@RunWith(Suite.class)
@Suite.SuiteClasses({
        FrontEndTest.class,
        BufferManagerTest.class,
        LRUBufferTest.class,
        NormalDataDiskManagerTest.class,
        MAIDDataDiskManagerTest.class,
        BlockTest.class,
        BufferRegionTest.class,
        RAPoSDABufferManagerTest.class,
        RAPoSDAStorageManagerTest.class,
        MAIDStorageManagerTest.class,
})

public class YKStorageTestSuite {
}
