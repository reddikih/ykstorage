package test.jp.ac.titech.cs.de.ykstorage.storage.datadisk;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.Block;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.MAIDDataDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.diskstate.StateManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@RunWith(JUnit4.class)
public class MAIDDataDiskManagerTest {

    private Parameter parameter = new Parameter("./test/test/jp/ac/titech/cs/de/ykstorage/storage/datadisk/maidtest.properties");

    private ArrayList<Block> blocks = new ArrayList<>();

    private StateManager getStateManager() {
        return new StateManager(
                parameter.devicePathPrefix,
                parameter.driveCharacters,
                parameter.spindownThresholdTime);
    }

    private MAIDDataDiskManager getDataDiskManager(int numberOfDataDisks) {
        return new MAIDDataDiskManager(
                numberOfDataDisks,
                parameter.diskFilePathPrefix,
                parameter.devicePathPrefix,
                parameter.driveCharacters,
                getStateManager());
    }

    private Block getBlock(long blockId, int diskId, String content) {
        if (content == null) content = "test content";

        byte[] payload;

        try {
            payload = content.getBytes("utf8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("invalid character encoding.");
        }
        return new Block(blockId, 0, diskId, 0, payload);
    }

    @Test
    public void writeABlockToDataDisk() {
        int numberOfDataDisks = 3;
        MAIDDataDiskManager dataDiskManager = getDataDiskManager(numberOfDataDisks);
        dataDiskManager.setDeleteOnExit(true);

        Block block = getBlock(1, dataDiskManager.assignPrimaryDiskId(1), "write to dataDisk test.");
        this.blocks.add(block);
        boolean result = dataDiskManager.write(blocks);
        assertThat(result, is(true));
    }

    @Test
    public void write16BlocksToDataDisk() {
        int numberOfDataDisks = 3;
        MAIDDataDiskManager dataDiskManager = getDataDiskManager(numberOfDataDisks);
        dataDiskManager.setDeleteOnExit(true);

        for (int i=0; i < 16; i++) {
            blocks.add(getBlock(i, dataDiskManager.assignPrimaryDiskId(i), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
        }

        boolean result = dataDiskManager.write(blocks);
        assertThat(result, is(true));
    }

    @Test
    public void oneWriteThenOneRead() {
        int numberOfDataDisks = 3;
        MAIDDataDiskManager dataDiskManager = getDataDiskManager(numberOfDataDisks);
        dataDiskManager.setDeleteOnExit(true);

        long blockId = 1;
        String content = "write after read is ok?";

        Block block = getBlock(blockId, dataDiskManager.assignPrimaryDiskId(blockId), content);
        this.blocks.add(block);
        boolean written = dataDiskManager.write(blocks);
        assertThat(written, is(true));

        ArrayList<Long> blockIds = new ArrayList<>();
        blockIds.add(blockId);
        List<Block> readBlocks = dataDiskManager.read(blockIds);
        assertThat(readBlocks.get(0).getBlockId(), is(blockId));
        assertThat(new String(readBlocks.get(0).getPayload()), is(content));
    }


}
