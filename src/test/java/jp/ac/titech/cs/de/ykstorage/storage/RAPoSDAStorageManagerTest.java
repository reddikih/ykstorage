package jp.ac.titech.cs.de.ykstorage.storage;

import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.Block;
import jp.ac.titech.cs.de.ykstorage.storage.RAPoSDAStorageManager;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.IRAPoSDABufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.ICacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.IStoppableDataDiskManager;
//import org.jmock.Expectations;
//import org.jmock.Sequence;
//import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;
import util.UnitTestUtility;

import java.util.Collection;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class RAPoSDAStorageManagerTest {
// TODO replace to jmockito
//
//    @Rule
//    public JUnitRuleMockery context = new JUnitRuleMockery();
//
//    private final static Block dummyBlock = new Block(0, 0, 0, 0, 0, new byte[0]);
//    private final static String CONF_PATH = "./config/test/raposda_sm_unittest.properties";
//
//    @Test
//    public void createRAPoSDAStorageManagerInstance() {
//        final IRAPoSDABufferManager bufferManager = context.mock(IRAPoSDABufferManager.class);
//        final ICacheDiskManager cacheDiskManager = context.mock(ICacheDiskManager.class);
//        final IStoppableDataDiskManager dataDiskManager = context.mock(IStoppableDataDiskManager.class);
//
//        final Parameter parameter = UnitTestUtility.getParameter(CONF_PATH);
//
//        context.checking(new Expectations() {{
//            allowing(dataDiskManager).startWatchDog();
//        }});
//
//        RAPoSDAStorageManager sm = new RAPoSDAStorageManager(
//                bufferManager,
//                cacheDiskManager,
//                dataDiskManager,
//                parameter,
//                parameter.numberOfReplicas);
//    }
//
//    @Test
//    public void writeOneBlockToBuffer() {
//        // set up context
//        final IRAPoSDABufferManager bufferManager = context.mock(IRAPoSDABufferManager.class);
//        final ICacheDiskManager cacheDiskManager = context.mock(ICacheDiskManager.class);
//        final IStoppableDataDiskManager dataDiskManager = context.mock(IStoppableDataDiskManager.class);
//
//        // expectations setting
//        context.checking(new Expectations() {{
//            allowing(dataDiskManager);
//            oneOf(bufferManager).write(with(any(Block.class))); will(returnValue(dummyBlock));
//        }});
//
//        // test code
//        Parameter parameter = UnitTestUtility.getParameter(CONF_PATH);
//        RAPoSDAStorageManager sm = new RAPoSDAStorageManager(
//                bufferManager,
//                cacheDiskManager,
//                dataDiskManager,
//                parameter,
//                parameter.numberOfReplicas);
//        UnitTestUtility.setBufferConfiguration(1,1,parameter);
//        byte[] content = UnitTestUtility.generateContent(1, (byte)1);
//
//        // assertion
//        boolean result = sm.write(0L, content);
//        assertThat(result, is(true));
//    }
//
//    @Test
//    public void writeTwoReplicasToBuffer() {
//        final IRAPoSDABufferManager bufferManager = context.mock(IRAPoSDABufferManager.class);
//        final ICacheDiskManager cacheDiskManager = context.mock(ICacheDiskManager.class);
//        final IStoppableDataDiskManager dataDiskManager = context.mock(IStoppableDataDiskManager.class);
//
//        // expectations setting
//        context.checking(new Expectations() {{
//            allowing(dataDiskManager);
//            exactly(2).of(bufferManager).write(with(any(Block.class))); will(returnValue(dummyBlock));
//        }});
//
//        Parameter parameter = UnitTestUtility.getParameter(CONF_PATH);
//        parameter.numberOfReplicas = 2; // set the number of replicas to two
//        RAPoSDAStorageManager sm = new RAPoSDAStorageManager(
//                bufferManager,
//                cacheDiskManager,
//                dataDiskManager,
//                parameter,
//                parameter.numberOfReplicas);
//        UnitTestUtility.setBufferConfiguration(4,1,parameter);
//        byte[] content = UnitTestUtility.generateContent(1, (byte)1);
//
//        // assertion
//        boolean result = sm.write(0L, content);
//        assertThat(result, is(true));
//    }
//
//    @Test
//    public void writeToDataDiskIfBufferWasOverflowed() {
//        final IRAPoSDABufferManager bufferManager = context.mock(IRAPoSDABufferManager.class);
//        final ICacheDiskManager cacheDiskManager = context.mock(ICacheDiskManager.class);
//        final IStoppableDataDiskManager dataDiskManager = context.mock(IStoppableDataDiskManager.class);
//
//        // expectations setting
//        context.checking(new Expectations() {{
//            final Sequence bufferOverflowed = context.sequence("bufferOverflowed");
//            exactly(1).of(bufferManager).write(with(any(Block.class))); will(returnValue(dummyBlock)); inSequence(bufferOverflowed);
//            exactly(1).of(bufferManager).write(with(any(Block.class))); will(returnValue(null)); inSequence(bufferOverflowed);
//            exactly(1).of(dataDiskManager).writeBlocks(with(any(Collection.class))); inSequence(bufferOverflowed);
//            allowing(dataDiskManager);
//            allowing(bufferManager);
//        }});
//
//        Parameter parameter = UnitTestUtility.getParameter(CONF_PATH);
//        parameter.numberOfReplicas = 2; // set the number of replicas to two
//        RAPoSDAStorageManager sm = new RAPoSDAStorageManager(
//                bufferManager,
//                cacheDiskManager,
//                dataDiskManager,
//                parameter,
//                parameter.numberOfReplicas);
//        UnitTestUtility.setBufferConfiguration(0,1,parameter);
//
//        byte[] content = UnitTestUtility.generateContent(1, (byte)1);
//
//        // assertion
//        boolean result = sm.write(0L, content);
//        assertThat(result, is(true));
//    }
}
