package jp.ac.titech.cs.de.ykstorage.storage.buffer;

import jp.ac.titech.cs.de.ykstorage.storage.Block;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.impl.RAPoSDABufferManager;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.assignor.CacheStripingAssignor;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.assignor.IAssignor;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.assignor.SimpleAssignor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

@RunWith(JUnit4.class)
public class RAPoSDABufferManagerTest {

    @Test
    public void configurationBufferManager() {
        int numberOfBuffer = 2;
        int replicaLevel = 1;
        IAssignor assignor = new SimpleAssignor(numberOfBuffer);

        RAPoSDABufferManager bufferManager = new RAPoSDABufferManager(numberOfBuffer, 10, 1, 1.0, replicaLevel, assignor);
        assertThat(bufferManager.getNumberOfBuffers(), is(numberOfBuffer));
        assertThat(bufferManager.getNumberOfRegions(), is(numberOfBuffer * replicaLevel));

        numberOfBuffer = 3; replicaLevel = 2;
        bufferManager = new RAPoSDABufferManager(numberOfBuffer, 10, 1, 1.0, replicaLevel, assignor);
        assertThat(bufferManager.getNumberOfBuffers(), is(numberOfBuffer));
        assertThat(bufferManager.getNumberOfRegions(), is(numberOfBuffer * replicaLevel));
    }

    @Test
    public void writeSomeBlocks() {
        int numberOfBuffer = 2;
        int replicaLevel = 2;
        int totalCapacity = 8;
        int blockSize = 1;
        IAssignor assignor = new CacheStripingAssignor(numberOfBuffer);

        RAPoSDABufferManager bufferManager = new RAPoSDABufferManager(numberOfBuffer, totalCapacity, blockSize, 1.0, replicaLevel, assignor);

        Block b0 = new Block(0, 0, 0, 0, 0, new byte[0]);
        assertThat(bufferManager.write(b0), is(b0));
        Block b1 = new Block(1, 0, 0, 0, 0, new byte[0]);
        assertThat(bufferManager.write(b1), is(b1));

        Block b2 = new Block(2, 0, 1, 0, 0, new byte[0]);
        assertThat(bufferManager.write(b2), is(b2));
        Block b3 = new Block(3, 0, 1, 0, 0, new byte[0]);
        assertThat(bufferManager.write(b3), is(b3));

        Block b4 = new Block(4, 1, 0, 0, 0, new byte[0]);
        assertThat(bufferManager.write(b4), is(b4));
        Block b5 = new Block(5, 1, 0, 0, 0, new byte[0]);
        assertThat(bufferManager.write(b5), is(b5));

        Block b6 = new Block(6, 1, 1, 0, 0, new byte[0]);
        assertThat(bufferManager.write(b6), is(b6));
        Block b7 = new Block(7, 1, 1, 0, 0, new byte[0]);
        assertThat(bufferManager.write(b7), is(b7));

    }

    @Test
    public void writeOverflow() {
        int numberOfBuffer = 2;
        int replicaLevel = 2;
        int totalCapacity = 8;
        int blockSize = 1;
        IAssignor assignor = new CacheStripingAssignor(numberOfBuffer);

        RAPoSDABufferManager bufferManager = new RAPoSDABufferManager(numberOfBuffer, totalCapacity, blockSize, 1.0, replicaLevel, assignor);

        Block b0 = new Block(0, 0, 0, 0, 0, new byte[0]);
        assertThat(bufferManager.write(b0), is(b0));
        Block b1 = new Block(1, 0, 0, 0, 0, new byte[0]);
        assertThat(bufferManager.write(b1), is(b1));

        // this will be failed to write due to overflow.
        Block b2 = new Block(2, 0, 0, 0, 0, new byte[0]);
        assertThat(bufferManager.write(b2), nullValue());

        // update write will be success.
        Block b3 = new Block(0, 0, 0, 0, 0, new byte[]{'b'});
        assertThat(b3, not(b0));
        assertThat(bufferManager.write(b3), is(b0));
    }

    @Test
    public void removeABlockFromFulfilledRegionThenWriteABlock() {
        int numberOfBuffer = 2;
        int replicaLevel = 2;
        int totalCapacity = 8;
        int blockSize = 1;
        IAssignor assignor = new CacheStripingAssignor(numberOfBuffer);

        RAPoSDABufferManager bufferManager = new RAPoSDABufferManager(numberOfBuffer, totalCapacity, blockSize, 1.0, replicaLevel, assignor);

        Block b0 = new Block(0, 0, 0, 0, 0, new byte[0]);
        assertThat(bufferManager.write(b0), is(b0));
        Block b1 = new Block(1, 0, 0, 0, 0, new byte[0]);
        assertThat(bufferManager.write(b1), is(b1));

        // this will be failed to write due to overflow.
        Block b2 = new Block(2, 0, 0, 0, 0, new byte[0]);
        assertThat(bufferManager.write(b2), nullValue());

        // remove a block from fulfilled buffer region.
        assertThat(bufferManager.remove(b1), is(b1));

        // this will be succeeded because there is free
        // space in the buffer region.
        assertThat(bufferManager.write(b2), is(b2));
    }

    @Test
    public void getBlockIdsCorrespondingToDisk() {
        int numberOfBuffer = 2;
        int replicaLevel = 2;
        int totalCapacity = 8;
        int blockSize = 1;
        IAssignor assignor = new CacheStripingAssignor(numberOfBuffer);

        RAPoSDABufferManager bufferManager =
                new RAPoSDABufferManager(numberOfBuffer, totalCapacity, blockSize, 1.0, replicaLevel, assignor);

        Block p0 = new Block(0, 0, 0, 0, 0, new byte[0]);
        Block p1 = new Block(1, 0, 1, 0, 1, new byte[0]);
        Block p2 = new Block(2, 0, 2, 0, 2, new byte[0]);
        Block p3 = new Block(3, 0, 3, 0, 3, new byte[0]);
        assertThat(bufferManager.write(p0), is(p0));
        assertThat(bufferManager.write(p1), is(p1));
        assertThat(bufferManager.write(p2), is(p2));
        assertThat(bufferManager.write(p3), is(p3));

        Block b0 = new Block(4, 1, 0, 0, 1, new byte[0]);
        Block b1 = new Block(5, 1, 1, 0, 2, new byte[0]);
        Block b2 = new Block(6, 1, 2, 0, 3, new byte[0]);
        Block b3 = new Block(7, 1, 3, 0, 0, new byte[0]);
        assertThat(bufferManager.write(b0), is(b0));
        assertThat(bufferManager.write(b1), is(b1));
        assertThat(bufferManager.write(b2), is(b2));
        assertThat(bufferManager.write(b3), is(b3));

        Block p4 = new Block(8, 0, 0, 0, 0, new byte[0]);
        assertThat(bufferManager.write(p4), nullValue());

        List<Block> corresponds =
                bufferManager.getBlocksCorrespondingToSpecifiedDisk(p4.getOwnerDiskId());
        List<Long> blockIdsInTheSameDisk = new ArrayList<>();
        blockIdsInTheSameDisk.add(0L);
        blockIdsInTheSameDisk.add(7L);
        for (Block b : corresponds) {
            assertThat(blockIdsInTheSameDisk.contains(b.getBlockId()), is(true));
        }
        assertThat(corresponds.size(), is(blockIdsInTheSameDisk.size()));
    }
}
