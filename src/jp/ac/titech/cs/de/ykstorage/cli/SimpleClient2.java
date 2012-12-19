package jp.ac.titech.cs.de.ykstorage.cli;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import jp.ac.titech.cs.de.ykstorage.service.DiskManager;
import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.service.StorageManager;
import jp.ac.titech.cs.de.ykstorage.service.cmm.CacheMemoryManager;
import jp.ac.titech.cs.de.ykstorage.util.StorageLogger;


public class SimpleClient2 {
	private static final String WORKLOAD_PATH = "C:\\Users\\oguri\\Desktop\\Book3.csv";
	private StorageManager sm;

	public SimpleClient2() {
		init();
	}

	private void init() {
		int capacity = Parameter.CAPACITY_OF_CACHEMEMORY;
		double threshold = Parameter.MEMORY_THRESHOLD;
		CacheMemoryManager cmm = new CacheMemoryManager(capacity, threshold);

		String[] diskPaths = Parameter.DATA_DISK_PATHS;
		String savePath = Parameter.DATA_DISK_SAVE_FILE_PATH;
		DiskManager dm = new DiskManager(
				diskPaths,
				savePath,
				Parameter.MOUNT_POINT_PATHS,
				Parameter.SPIN_DOWN_THRESHOLD);

		this.sm = new StorageManager(cmm, dm);

		StorageLogger.getLogger().config("Starting Simple Clinet.");
	}

	public boolean put(String key, String value) {
		byte[] byteVal = value.getBytes();
		return sm.put(key, byteVal);
	}

	public Object get(String key) {
		byte[] byteVal = sm.get(key);
		String value = new String(byteVal);
		return value;
	}


	public static void main(String[] args) throws IOException, InterruptedException {
		SimpleClient2 sc = new SimpleClient2();
		
		File f = new File(WORKLOAD_PATH);
		FileReader fr = new FileReader(f);
		BufferedReader br = new BufferedReader(fr);
		String line = "";
		int interval = 0;

		while((line = br.readLine()) != null) {
			interval = 0;
			String[] cmdArray = line.split(",");
			
			if(cmdArray.length == 3 && (cmdArray[0].equalsIgnoreCase("get"))) {
				System.out.print("[GET] Key: " + cmdArray[1]);
				System.out.println(" Value: " + sc.get(cmdArray[1]));
				interval = Integer.parseInt(cmdArray[2]);
			}

			if(cmdArray.length == 4 && (cmdArray[0].equalsIgnoreCase("PUT"))) {
				if(sc.put(cmdArray[1], cmdArray[2])) {
					System.out.print("[PUT] Key: " + cmdArray[1]);
					System.out.println(" Value: " + cmdArray[2]);
				}else {
					System.out.println("Cannot put");
				}
				interval = Integer.parseInt(cmdArray[3]);
			}
			
			Thread.sleep(interval);
		}
		
		br.close();
	}

}
