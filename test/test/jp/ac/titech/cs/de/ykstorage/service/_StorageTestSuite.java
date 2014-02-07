package test.jp.ac.titech.cs.de.ykstorage.service;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import test.jp.ac.titech.cs.de.ykstorage.storage.*;
import test.jp.ac.titech.cs.de.ykstorage.storage.buffer.CacheMemoryManagerTest;
import test.jp.ac.titech.cs.de.ykstorage.storage.cachedisk.MAIDCacheDiskManagerTest;
import test.jp.ac.titech.cs.de.ykstorage.storage.cachedisk.ReCacheDiskManagerTest;
import test.jp.ac.titech.cs.de.ykstorage.storage.datadisk.MAIDDataDiskManagerTest;
import test.jp.ac.titech.cs.de.ykstorage.storage.datadisk.ReDataDiskManagerTest;
import test.jp.ac.titech.cs.de.ykstorage.storage.diskstate.StateManagerTest;

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
	ReStorageManagerTest.class,
	ReCacheDiskManagerTest.class,
	ReDataDiskManagerTest.class
})

public class _StorageTestSuite {}
