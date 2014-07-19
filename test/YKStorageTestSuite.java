import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import test.jp.ac.titech.cs.de.ykstorage.frontend.FrontEndTest;
import test.jp.ac.titech.cs.de.ykstorage.storage.BlockTest;
import test.jp.ac.titech.cs.de.ykstorage.storage.MAIDStorageManagerTest;
import test.jp.ac.titech.cs.de.ykstorage.storage.RAPoSDAStorageManagerTest;
import test.jp.ac.titech.cs.de.ykstorage.storage.buffer.BufferManagerTest;
import test.jp.ac.titech.cs.de.ykstorage.storage.buffer.BufferRegionTest;
import test.jp.ac.titech.cs.de.ykstorage.storage.buffer.LRUBufferTest;
import test.jp.ac.titech.cs.de.ykstorage.storage.buffer.RAPoSDABufferManagerTest;
import test.jp.ac.titech.cs.de.ykstorage.storage.datadisk.MAIDDataDiskManagerTest;
import test.jp.ac.titech.cs.de.ykstorage.storage.datadisk.NormalDataDiskManagerTest;

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
