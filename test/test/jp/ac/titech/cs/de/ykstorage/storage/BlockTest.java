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

        Block b3 = new Block(1, 0, 0, 0, -1, new byte[0]);
        assertThat(b1.equals(b3), is(true));
    }

    @Test
    public void notEquality() {
        Block b1 = new Block(1, 0, 0, 0, new byte[0]);

        Block b2 = new Block(2, 0, 0, 0, new byte[0]);
        assertThat(b1.equals(b2), is(false));
        Block b3 = new Block(1, 1, 0, 0, new byte[0]);
        assertThat(b1.equals(b3), is(false));
        Block b4 = new Block(1, 0, 1, 0, new byte[0]);
        assertThat(b1.equals(b4), is(false));
        Block b5 = new Block(1, 0, 0, 1, new byte[0]);
        assertThat(b1.equals(b5), is(false));
        Block b6 = new Block(1, 0, 0, 0, 1, new byte[0]);
        assertThat(b1.equals(b6), is(false));
        Block b7 = new Block(1, 0, 0, 0, new byte[3]);
        assertThat(b1.equals(b7), is(false));
    }

    @Test
    public void hashCodeEquality() {
        Block b1 = new Block(1, 0, 0, 0, new byte[0]);
        Block b2 = new Block(1, 0, 0, 0, new byte[0]);
        assertThat(b1.hashCode(), is(b2.hashCode()));

        Block b3 = new Block(1, 0, 0, 0, -1, new byte[0]);
        assertThat(b1.hashCode(), is(b3.hashCode()));
    }

}
