package test.jp.ac.titech.cs.de.ykstorage.storage;

import jp.ac.titech.cs.de.ykstorage.storage.Block;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@RunWith(JUnit4.class)
public class BlockTest {

    @Test
    public void equalsTest() {
        Block b1 = new Block(1, 0, 0, 0, new byte[0]);
        Block b2 = new Block(1, 0, 0, 0, new byte[0]);
        assertThat(b1.equals(b2), is(true));
    }

}
