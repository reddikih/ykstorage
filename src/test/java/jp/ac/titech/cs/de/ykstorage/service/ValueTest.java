package jp.ac.titech.cs.de.ykstorage.service;

import static org.junit.Assert.*;
import jp.ac.titech.cs.de.ykstorage.service.Value;

import org.junit.Test;

public class ValueTest {

	@Test
	public void testEqualsObject() {
		Value val1 = new Value(new byte[]{0,1,2});
		Value val2 = new Value(new byte[]{0,1,2});
		Value val3 = new Value(new byte[]{1,2,3});
		assertEquals(val1, val2);
		assertTrue(val1.equals(val2));
		assertFalse(val2.equals(val3));
	}

}
