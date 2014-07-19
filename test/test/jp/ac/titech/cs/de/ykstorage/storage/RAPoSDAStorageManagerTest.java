package test.jp.ac.titech.cs.de.ykstorage.storage;

import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.RAPoSDAStorageManager;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.IRAPoSDABufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.ICacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.IStoppableDataDiskManager;
import static org.hamcrest.core.Is.is;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import static org.junit.Assert.assertThat;
import org.junit.Rule;
import org.junit.Test;
import test.util.UnitTestUtility;

public class RAPoSDAStorageManagerTest {

    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery();

    private final static String CONF_PATH = "./config/test/raposda_sm_unittest.properties";

    @Test
    public void createRAPoSDAStorageManagerInstance() {
        final IRAPoSDABufferManager bufferManager = context.mock(IRAPoSDABufferManager.class);
        final ICacheDiskManager cacheDiskManager = context.mock(ICacheDiskManager.class);
        final IStoppableDataDiskManager dataDiskManager = context.mock(IStoppableDataDiskManager.class);

        final Parameter parameter = UnitTestUtility.getParameter(CONF_PATH);

        context.checking(new Expectations() {{
            allowing(dataDiskManager).startWatchDog();
        }});

        RAPoSDAStorageManager sm = new RAPoSDAStorageManager(
                bufferManager,
                cacheDiskManager,
                dataDiskManager,
                parameter,
                parameter.numberOfReplicas);
    }

    @Test
    public void writeOneBlockToBuffer() {
        // set up context
        final IRAPoSDABufferManager bufferManager = context.mock(IRAPoSDABufferManager.class);
        final ICacheDiskManager cacheDiskManager = context.mock(ICacheDiskManager.class);
        final IStoppableDataDiskManager dataDiskManager = context.mock(IStoppableDataDiskManager.class);

        // expectations setting
        context.checking(new Expectations() {{
            allowing(dataDiskManager);
            allowing(bufferManager);
        }});

        // test code
        final Parameter parameter = UnitTestUtility.getParameter(CONF_PATH);
        RAPoSDAStorageManager sm = new RAPoSDAStorageManager(
                bufferManager,
                cacheDiskManager,
                dataDiskManager,
                parameter,
                parameter.numberOfReplicas);
        UnitTestUtility.setBufferConfiguration(1,1,parameter);
        byte[] content = UnitTestUtility.generateContent(1, (byte)1);

        // assertion
        assertThat(sm.write(0L, content), is(true));
    }
}
