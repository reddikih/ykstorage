package test.jp.ac.titech.cs.de.ykstorage.storage.buffer;

import jp.ac.titech.cs.de.ykstorage.storage.Block;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.BufferRegion;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

@RunWith(JUnit4.class)
public class BufferRegionTest {

    @Test
    public void writeTest() {
        BufferRegion region = new BufferRegion(0, 0, 3);

        Block b0 = new Block(0, 0, 0, 0, new byte[0]);
        assertThat(region.write(b0), is(b0));
        Block b1 = new Block(1, 0, 0, 0, new byte[0]);
        assertThat(region.write(b1), is(b1));
        assertThat(region.write(b1), not(b0));
        Block b2 = new Block(2, 0, 0, 0, new byte[0]);
        assertThat(region.write(b2), is(b2));
        assertThat(region.write(b2), not(b0));

        // This is null result due to buffer overflow
        Block b3 = new Block(3, 0, 0, 0, new byte[0]);
        assertThat(region.write(b3), nullValue());
    }

    @Test
    public void readTest() {
        BufferRegion region = new BufferRegion(0, 0, 3);

        Block b0 = new Block(0, 0, 0, 0, new byte[0]);
        assertThat(region.write(b0), is(b0));
        Block b1 = new Block(1, 0, 0, 0, new byte[0]);
        assertThat(region.write(b1), is(b1));
        Block b2 = new Block(2, 0, 0, 0, new byte[0]);
        assertThat(region.write(b2), is(b2));

        assertThat(region.read(b0.getBlockId()), is(b0));
        assertThat(region.read(b1.getBlockId()), is(b1));
        assertThat(region.read(b2.getBlockId()), is(b2));
    }

    @Test
    public void readNonExistenceBlock() {
        BufferRegion region = new BufferRegion(0, 0, 3);

        Block b0 = new Block(0, 0, 0, 0, new byte[0]);
        assertThat(region.write(b0), is(b0));

        assertThat(region.read(100), nullValue());
    }

    @Test
    public void removeTest() {
        BufferRegion region = new BufferRegion(0, 0, 3);

        Block b0 = new Block(0, 0, 0, 0, new byte[0]);
        assertThat(region.write(b0), is(b0));

        assertThat(region.remove(b0.getBlockId()), is(b0));

        // return null when removing non-existence block id.
        assertThat(region.remove(100), nullValue());

    }

    @Test
    public void filledBufferThenRemoveAndWriteAnotherBlock() {
        BufferRegion region = new BufferRegion(0, 0, 3);

        Block b0 = new Block(0, 0, 0, 0, new byte[0]);
        assertThat(region.write(b0), is(b0));
        Block b1 = new Block(1, 0, 0, 0, new byte[0]);
        assertThat(region.write(b1), is(b1));
        Block b2 = new Block(2, 0, 0, 0, new byte[0]);
        assertThat(region.write(b2), is(b2));

        // write failed due to buffer overflow
        Block b3 = new Block(3, 0, 0, 0, new byte[0]);
        assertThat(region.write(b3), nullValue());

        // remove a block and make a room to store another block
        assertThat(region.remove(b0.getBlockId()), is(b0));
        assertThat(region.read(b0.getBlockId()), nullValue());

        // write a new block
        assertThat(region.write(b3), is(b3));
        assertThat(region.read(b3.getBlockId()), is(b3));
        assertThat(region.read(b2.getBlockId()), is(b2));

    }
}
