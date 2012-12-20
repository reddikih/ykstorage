package jp.ac.titech.cs.de.ykstorage.cli;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import jp.ac.titech.cs.de.ykstorage.service.DiskManager;
import jp.ac.titech.cs.de.ykstorage.service.MAIDCacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.service.MAIDDataDiskManager;
import jp.ac.titech.cs.de.ykstorage.service.MAIDStorageManager;
import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.service.StorageManager;
import jp.ac.titech.cs.de.ykstorage.service.cmm.CacheMemoryManager;
import jp.ac.titech.cs.de.ykstorage.util.StorageLogger;

public class MAIDSimpleClient2 {
	private static final String[] WORKLOAD_PATH = {"C:\\Users\\oguri\\Desktop\\Book1.csv"
													, "C:\\Users\\oguri\\Desktop\\Book2.csv"
													, "C:\\Users\\oguri\\Desktop\\Book3.csv"};
	private static final int cmdIndex = 0;
	private static final int intervalIndex = 1;
	private static final int keylIndex = 2;
	private static final int valuelIndex = 3;
	
	private MAIDStorageManager sm;

	public MAIDSimpleClient2() {
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
				Parameter.SPIN_DOWN_THRESHOLD);

		this.sm = new MAIDStorageManager(cmm, cdm, ddm);

		StorageLogger.getLogger().config("Starting Simple Clinet.");
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
	
	public void loadWorkload(MAIDSimpleClient2 sc, String filePath) throws IOException, InterruptedException {
		File f = new File(filePath);
		FileReader fr = new FileReader(f);
		BufferedReader br = new BufferedReader(fr);
		String line = "";
		int interval = 0;

		while((line = br.readLine()) != null) {
			interval = 0;
			String[] cmdArray = line.split(",");
			
			if(cmdArray.length == 3 && (cmdArray[cmdIndex].equalsIgnoreCase("get"))) {
				System.out.print("[GET] Key: " + cmdArray[keylIndex]);
				System.out.println(" Value: " + sc.get(cmdArray[keylIndex]));
				interval = Integer.parseInt(cmdArray[intervalIndex]);
			}

			if(cmdArray.length == 4 && (cmdArray[cmdIndex].equalsIgnoreCase("PUT"))) {
				if(sc.put(cmdArray[keylIndex], cmdArray[valuelIndex])) {
					System.out.print("[PUT] Key: " + cmdArray[keylIndex]);
					System.out.println(" Value: " + cmdArray[valuelIndex]);
				}else {
					System.out.println("Cannot put");
				}
				interval = Integer.parseInt(cmdArray[intervalIndex]);
			}
			
			Thread.sleep(interval);
		}
		
		br.close();
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		MAIDSimpleClient2 sc = new MAIDSimpleClient2();
		
		sc.loadWorkload(sc, WORKLOAD_PATH[0]);
		
		Thread.sleep(1000);
		
		sc.end();
		MAIDSimpleClient2 sc2 = new MAIDSimpleClient2();
		sc2.loadWorkload(sc2, WORKLOAD_PATH[1]);
	}

}
