package jp.ac.titech.cs.de.ykstorage.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import jp.ac.titech.cs.de.ykstorage.storage.OLDMAIDStorageManager;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.OLDMAIDCacheDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.cachedisk.MAIDCacheDiskStateManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.OLDMAIDDataDiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.datadisk.MAIDDataDiskStateManager;
import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.CacheMemoryManager;


public class MAIDSimpleClient {
	private OLDMAIDStorageManager sm;

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
		
		MAIDDataDiskStateManager ddsm = new MAIDDataDiskStateManager(Parameter.MOUNT_POINT_PATHS, Parameter.DATA_DISK_PATHS,
				Parameter.SPIN_DOWN_THRESHOLD, Parameter.SPINDOWN_INTERVAL, Parameter.RMI_URL,
				Parameter.IS_CACHEDISK, Parameter.NUMBER_OF_CACHE_DISKS, Parameter.NUMBER_OF_DATA_DISKS,
				Parameter.ACC);
		
		OLDMAIDDataDiskManager ddm = new OLDMAIDDataDiskManager(
				dataDiskPaths,
				savePath,
				Parameter.MOUNT_POINT_PATHS,
				Parameter.SPIN_DOWN_THRESHOLD,
				ddsm);
		
		MAIDCacheDiskStateManager cdsm = new MAIDCacheDiskStateManager(Parameter.MOUNT_POINT_PATHS, Parameter.CACHE_DISK_PATHS,
				Parameter.ACCESS_THRESHOLD, Parameter.ACCESS_INTERVAL, Parameter.RMI_URL,
				Parameter.IS_CACHEDISK, Parameter.NUMBER_OF_CACHE_DISKS, Parameter.NUMBER_OF_DATA_DISKS);
		
		OLDMAIDCacheDiskManager cdm = new OLDMAIDCacheDiskManager(
				cacheDiskPaths,
				savePath,
				Parameter.MOUNT_POINT_PATHS,
				Parameter.SPIN_DOWN_THRESHOLD,
				Parameter.CAPACITY_OF_CACHEDISK,
				cdsm);

		this.sm = new OLDMAIDStorageManager(cmm, cdm, ddm);
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
		int bufSize = 1024 * 1024 * 20;
		char[] buf = new char[bufSize];
		String bufValue = String.valueOf(buf);
		
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
					System.out.println(" Value: " + cmdArray[2] + " size: " + cmdArray[2].length() + "[B]");
				}else {
					System.out.println("Cannot put");
				}
			}
			
			if(cmdArray.length == 3 && (cmdArray[0].equalsIgnoreCase("PUTL"))) {
				int valueSize = Integer.parseInt(cmdArray[2]); // [B]
				String value = bufValue.substring(0, valueSize);
				
				if(sc.put(cmdArray[1], value)) {
					System.out.print("[PUTL] Key: " + cmdArray[1]);
					System.out.println(" Value: " + valueSize + "[B]");
				}else {
					System.out.println("Cannot put");
				}
			}
		}
	}

}
