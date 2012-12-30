package jp.ac.titech.cs.de.ykstorage.cli;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import jp.ac.titech.cs.de.ykstorage.service.MAIDCacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.service.MAIDCacheDiskStateManager;
import jp.ac.titech.cs.de.ykstorage.service.MAIDDataDiskManager;
import jp.ac.titech.cs.de.ykstorage.service.MAIDStorageManager;
import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.service.cmm.CacheMemoryManager;
import jp.ac.titech.cs.de.ykstorage.util.StorageLogger;

public class MAIDSimpleClient2 {
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
		
		MAIDCacheDiskStateManager sm = new MAIDCacheDiskStateManager(Parameter.MOUNT_POINT_PATHS, Parameter.CACHE_DISK_PATHS,
				Parameter.ACCESS_THRESHOLD, Parameter.ACCESS_INTERVAL, Parameter.RMI_URL,
				Parameter.IS_CACHEDISK, Parameter.NUMBER_OF_CACHE_DISKS, Parameter.NUMBER_OF_DATA_DISKS);
		
		MAIDCacheDiskManager cdm = new MAIDCacheDiskManager(
				cacheDiskPaths,
				savePath,
				Parameter.MOUNT_POINT_PATHS,
				Parameter.SPIN_DOWN_THRESHOLD,
				Parameter.CAPACITY_OF_CACHEDISK,
				sm);

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
		int bufSize = 1024 * 1024 * 20;
		char[] buf = new char[bufSize];
		String bufValue = String.valueOf(buf);
		
		File f = new File(filePath);
		FileReader fr = new FileReader(f);
		BufferedReader br = new BufferedReader(fr);
		String line = "";
		int interval = 0;

		int i = 0;
		while((line = br.readLine()) != null) {
			interval = 0;
			String[] cmdArray = line.split(",");
			
			i++;
			System.out.print(i + " ");
			if(cmdArray.length == 3 && (cmdArray[cmdIndex].equalsIgnoreCase("get"))) {
				System.out.print("[GET] Key: " + cmdArray[keylIndex]);
				String value = (String) sc.get(cmdArray[keylIndex]);
				if(value.length() < 20) {
					System.out.println(" Value: " + value);
				} else {
					System.out.println(" Value: " + value.substring(0, 20) + "...");
				}
				interval = Integer.parseInt(cmdArray[intervalIndex]);
			}

			if(cmdArray.length == 4 && (cmdArray[cmdIndex].equalsIgnoreCase("PUT"))) {
				if(sc.put(cmdArray[keylIndex], cmdArray[valuelIndex])) {
					System.out.print("[PUT] Key: " + cmdArray[keylIndex]);
					System.out.println(" Value: " + cmdArray[valuelIndex] + " size: " + cmdArray[valuelIndex].length() + "[B]");
				}else {
					System.out.println("Cannot put");
				}
				interval = Integer.parseInt(cmdArray[intervalIndex]);
			}
			
			if(cmdArray.length == 4 && (cmdArray[cmdIndex].equalsIgnoreCase("PUTL"))) {
				int valueSize = Integer.parseInt(cmdArray[valuelIndex]); // [B]
				String value = bufValue.substring(0, valueSize);
				
				if(sc.put(cmdArray[keylIndex], value)) {
					System.out.print("[PUTL] Key: " + cmdArray[keylIndex]);
					System.out.println(" Value: " + valueSize + "[B]");
				}else {
					System.out.println("Cannot put");
				}
				interval = Integer.parseInt(cmdArray[intervalIndex]);
			}
			
			Thread.sleep(interval);
		}
		
		br.close();
		System.out.println("finished");
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		MAIDSimpleClient2 sc = new MAIDSimpleClient2();
		
		sc.loadWorkload(sc, args[0]);
		
//		sc.loadWorkload(sc, WORKLOAD_PATH[0]);
//		
//		Thread.sleep(1000);
//		
//		sc.end();
//		MAIDSimpleClient2 sc2 = new MAIDSimpleClient2();
//		sc2.loadWorkload(sc2, WORKLOAD_PATH[1]);
	}

}
