package test.jp.ac.titech.cs.de.ykstorage.storage.datadisk;

import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.Block;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.NormalDataDiskManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@RunWith(JUnit4.class)
public class NormalDataDiskManagerTest {

    private final static String diskFilePrefix =
            "./test/test/jp/ac/titech/cs/de/ykstorage/storage/datadisk/data/sd";
    private Parameter parameter = new Parameter();

    private ArrayList<Block> blocks = new ArrayList<Block>();

    private NormalDataDiskManager getDataDiskManager(int numberOfDataDisks) {
        return new NormalDataDiskManager(numberOfDataDisks, diskFilePrefix, parameter.driveCharacters);
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
        NormalDataDiskManager dataDiskManager = getDataDiskManager(numberOfDataDisks);
        dataDiskManager.setDeleteOnExit(true);

        Block block = getBlock(1, dataDiskManager.assginPrimaryDiskId(1), "write to dataDisk test.");
        this.blocks.add(block);
        boolean result = dataDiskManager.write(blocks);
        assertThat(result, is(true));
    }

    @Test
    public void write16BlocksToDataDisk() {
        int numberOfDataDisks = 3;
        NormalDataDiskManager dataDiskManager = getDataDiskManager(numberOfDataDisks);
        dataDiskManager.setDeleteOnExit(true);

        for (int i=0; i < 16; i++) {
            blocks.add(getBlock(i, dataDiskManager.assginPrimaryDiskId(i), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
        }

        boolean result = dataDiskManager.write(blocks);
        assertThat(result, is(true));
    }

    @Test
    public void oneWriteThenOneRead() {
        int numberOfDataDisks = 3;
        NormalDataDiskManager dataDiskManager = getDataDiskManager(numberOfDataDisks);
        dataDiskManager.setDeleteOnExit(true);

        long blockId = 1;
        String content = "write after read is ok?";

        Block block = getBlock(blockId, dataDiskManager.assginPrimaryDiskId(blockId), content);
        this.blocks.add(block);
        boolean written = dataDiskManager.write(blocks);
        assertThat(written, is(true));

        ArrayList<Long> blockIds = new ArrayList<Long>();
        blockIds.add(blockId);
        List<Block> readBlocks = dataDiskManager.read(blockIds);
        assertThat(readBlocks.get(0).getBlockId(), is(blockId));
        assertThat(new String(readBlocks.get(0).getPayload()), is(content));
    }

}
