package test.jp.ac.titech.cs.de.ykstorage.storage.buffer;

import jp.ac.titech.cs.de.ykstorage.storage.Block;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.BufferManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

@RunWith(JUnit4.class)
public class BufferManagerTest {

    private Block[] createBlocks(int num, int startId) {
        Block[] blocks = new Block[num];
        int s = startId;
        for (int i=0; i < blocks.length; i++) {
            blocks[i] = new Block(s++, 0, 0, 0, new byte[0]);
        }
        return blocks;
    }

    @Test
    public void insert() {
        BufferManager buffer = new BufferManager(10, 1, 1.0);
        Block result;
        Block[] blocks = createBlocks(10, 0);
        for (int i=0; i < blocks.length; i++) {
            result = buffer.write(blocks[i]);
            assertThat(result, nullValue());
        }

        result = buffer.write(new Block(11, 0, 0, 0, new byte[0]));
        assertThat(result, notNullValue());
        assertThat(result.getBlockId(), is(0L));
    }

    @Test
    public void insertThenRead() {
        BufferManager buffer = new BufferManager(10, 1, 1.0);
    }
}
