package jp.ac.titech.cs.de.ykstorage.service;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import jp.ac.titech.cs.de.ykstorage.storage.*;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.CacheMemoryManagerTest;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.OLDMAIDCacheDiskManagerTest;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.ReCacheDiskManagerTest;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.OLDMAIDDataDiskManagerTest;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.ReDataDiskManagerTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	DiskManagerTest.class,
	StorageManagerTest.class,
	StorageServiceTest.class,
	ValueTest.class,
	CacheMemoryManagerTest.class,
	OLDMAIDStorageManagerTest.class,
	OLDMAIDCacheDiskManagerTest.class,
	OLDMAIDDataDiskManagerTest.class,
	ReStorageManagerTest.class,
	ReCacheDiskManagerTest.class,
	ReDataDiskManagerTest.class
})

public class _StorageTestSuite {}
