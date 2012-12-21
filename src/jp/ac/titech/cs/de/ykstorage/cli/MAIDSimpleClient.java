package jp.ac.titech.cs.de.ykstorage.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import jp.ac.titech.cs.de.ykstorage.service.MAIDCacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.service.MAIDDataDiskManager;
import jp.ac.titech.cs.de.ykstorage.service.MAIDStorageManager;
import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.service.cmm.CacheMemoryManager;
import jp.ac.titech.cs.de.ykstorage.util.StorageLogger;


public class MAIDSimpleClient {
	private MAIDStorageManager sm;

	public MAIDSimpleClient() {
		init();
	}

	private void init() {
		int capacity = Parameter.CAPACITY_OF_CACHEMEMORY;
		double threshold = Parameter.MEMORY_THRESHOLD;
		CacheMemoryManager cmm = new CacheMemoryManager(capacity, threshold);

		String[] dataDiskPaths = Parameter.DATA_DISK_PATHS;
		String[] cacheDiskPaths = Parameter.CACHE_DISK_PATHS;
		String savePath = Parameter.DATA_DISK_SAVE_FILE_PATH;
		MAIDDataDiskManager ddm = new MAIDDataDiskManager(
				dataDiskPaths,
				savePath,
				Parameter.MOUNT_POINT_PATHS,
				Parameter.SPIN_DOWN_THRESHOLD);
		
		MAIDCacheDiskManager cdm = new MAIDCacheDiskManager(
				cacheDiskPaths,
				savePath,
				Parameter.MOUNT_POINT_PATHS,
				Parameter.SPIN_DOWN_THRESHOLD,
				Parameter.CAPACITY_OF_CACHEDISK);

		this.sm = new MAIDStorageManager(cmm, cdm, ddm);

		StorageLogger.getLogger().config("Starting MAID Simple Clinet.");
	}

	public boolean put(String key, String value) {
		byte[] byteVal = value.getBytes();
		return sm.put(key, byteVal);
	}

	public Object get(String key) {
		byte[] byteVal = sm.get(key);
		String value = new String(byteVal); //TODO set character code
		return value;
	}
	
	public void end() {
		sm.end();
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		MAIDSimpleClient sc = new MAIDSimpleClient();
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		while(true) {
			System.out.print("cmd: ");
			String cmd = br.readLine();
			String[] cmdArray = cmd.split(" ");

			if(cmdArray.length == 2 && (cmdArray[0].equalsIgnoreCase("get"))) {
				System.out.print("[GET] Key: " + cmdArray[1]);
				System.out.println(" Value: " + sc.get(cmdArray[1]));
			}

			if(cmdArray.length == 3 && (cmdArray[0].equalsIgnoreCase("PUT"))) {
				if(sc.put(cmdArray[1], cmdArray[2])) {
					System.out.print("[PUT] Key: " + cmdArray[1]);
					System.out.println(" Value: " + cmdArray[2]);
				}else {
					System.out.println("Cannot put");
				}
			}
		}
	}

}