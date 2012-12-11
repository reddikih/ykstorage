package test.jp.ac.titech.cs.de.ykstorage.service;

import static org.junit.Assert.*;
import jp.ac.titech.cs.de.ykstorage.service.Parameter;

import org.junit.Test;

public class ParameterTest {

	@Test
	public void printDataDiskPathsTest() {
		for (String path : Parameter.DATA_DISK_PATHS) {
			System.out.println(path);
		}
	}
	
	@Test
	public void printMountPointPathsTest() {
		for (String path : Parameter.DATA_DISK_PATHS) {
			System.out.println(Parameter.MOUNT_POINT_PATHS.get(path));
		}
	}
}
