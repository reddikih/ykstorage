package jp.ac.titech.cs.de.ykstorage.storage.buffer.assignor;

import org.junit.Test;
import sun.java2d.pipe.SpanShapeRenderer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

/**
 * Created by hikida on 2016/04/05.
 */
public class SimpleAssignorTest {

    @Test(expected = IllegalArgumentException.class)
    public void testZeroBufferThrowException() {
        SimpleAssignor sut = new SimpleAssignor(0);
        assertThat(sut.assign(1L, 1, 1), is(0));
    }

    @Test
    public void testAssignInModularMannerByOne() {
        SimpleAssignor sut = new SimpleAssignor(1);
        assertThat(sut.assign(1L, 1, 1), is(0));
        assertThat(sut.assign(2L, 1, 1), is(0));
        assertThat(sut.assign(100L, 1, 1), is(0));
        assertThat(sut.assign(1128L, 1128, 3), is(0));
        assertThat(sut.assign(99999L, -1, Integer.MAX_VALUE), is(0));
    }

    @Test
    public void testAssignInModularMannerByThree() {
        SimpleAssignor sut = new SimpleAssignor(3);
        assertThat(sut.assign(1L, 1, 1), is(1));
        assertThat(sut.assign(2L, 1, 1), is(2));
        assertThat(sut.assign(3L, 1, 1), is(0));
        assertThat(sut.assign(4L, 1, 1), is(1));
        assertThat(sut.assign(5L, 1, 1), is(2));
        assertThat(sut.assign(6L, 1, 1), is(0));
    }
}