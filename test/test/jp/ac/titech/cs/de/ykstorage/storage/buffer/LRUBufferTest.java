package test.jp.ac.titech.cs.de.ykstorage.storage.buffer;

import jp.ac.titech.cs.de.ykstorage.storage.Block;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.LRUBuffer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

@RunWith(JUnit4.class)
public class LRUBufferTest {

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
        LRUBuffer buffer = new LRUBuffer(3);
        Block result;

        Block[] blocks = createBlocks(4, 0);
        for (int i=0; i<blocks.length -1; i++) {
            result = buffer.add(blocks[i]);
            assertThat(result, nullValue());
        }
        result = buffer.add(blocks[blocks.length -1]);
        assertThat(result, notNullValue());
        assertThat(result, is(blocks[0]));
    }

    @Test
    public void replace() {
        LRUBuffer buffer = new LRUBuffer(3);
        Block result;

        Block[] blocks = createBlocks(3, 0);
        for (int i=0; i<blocks.length; i++) {
            result = buffer.add(blocks[i]);
            assertThat(result, nullValue());
        }

        result = buffer.add(new Block(3, 0, 0, 0, new byte[0]));
        assertThat(result, notNullValue());
        assertThat(result.getBlockId(), is(0L));

        result = buffer.add(new Block(1, 0, 0, 0, new byte[0]));
        assertThat(result, nullValue());

        result = buffer.add(new Block(4, 0, 0, 0, new byte[0]));
        assertThat(result, notNullValue());
        assertThat(result.getBlockId(), is(2L));

        result = buffer.add(new Block(5, 0, 0, 0, new byte[0]));
        assertThat(result, notNullValue());
        assertThat(result.getBlockId(), is(3L));
    }
}
