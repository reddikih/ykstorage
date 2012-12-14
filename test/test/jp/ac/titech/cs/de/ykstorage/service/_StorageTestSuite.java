package test.jp.ac.titech.cs.de.ykstorage.service;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import test.jp.ac.titech.cs.de.ykstorage.service.cmm.CacheMemoryManagerTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	DiskManagerTest.class,
	StorageManagerTest.class,
	StorageServiceTest.class,
	ValueTest.class,
	CacheMemoryManagerTest.class,
	StateManagerTest.class,
	MAIDStorageManagerTest.class,
	MAIDCacheDiskManagerTest.class,
	MAIDDataDiskManagerTest.class,
})

public class _StorageTestSuite {}
