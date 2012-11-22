package test.jp.ac.titech.cs.de.ykstorage.service;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import jp.ac.titech.cs.de.ykstorage.service.StorageManager;

@RunWith(JUnit4.class)
public class StorageManagerTest {
	
	private StorageManager sm;
	
	@BeforeClass
	public void setUpClass() {
		this.sm = new StorageManager();
	}
	

	@Test
	public void testGet() {
		
		fail("Not yet implemented");
	}

	@Test
	public void testPut() {
		fail("Not yet implemented");
	}

}
