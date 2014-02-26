import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import test.jp.ac.titech.cs.de.ykstorage.frontend.FrontEndTest;
import test.jp.ac.titech.cs.de.ykstorage.storage.BlockTest;
import test.jp.ac.titech.cs.de.ykstorage.storage.buffer.BufferManagerTest;
import test.jp.ac.titech.cs.de.ykstorage.storage.buffer.LRUBufferTest;
import test.jp.ac.titech.cs.de.ykstorage.storage.datadisk.MAIDDataDiskManagerTest;
import test.jp.ac.titech.cs.de.ykstorage.storage.datadisk.NormalDataDiskManagerTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        FrontEndTest.class,
        BufferManagerTest.class,
        LRUBufferTest.class,
        NormalDataDiskManagerTest.class,
        MAIDDataDiskManagerTest.class,
        BlockTest.class,
})

public class YKStorageTestSuite {
}
