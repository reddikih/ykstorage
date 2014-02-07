package jp.ac.titech.cs.de.ykstorage.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import jp.ac.titech.cs.de.ykstorage.service.DiskManager;
import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.service.StorageManager;
import jp.ac.titech.cs.de.ykstorage.service.cmm.CacheMemoryManager;
import jp.ac.titech.cs.de.ykstorage.util.StorageLogger;

public class SimpleClient {
	private StorageManager sm;

	public SimpleClient() {
		init();
	}

	private void init() {
		int capacity = Parameter.CAPACITY_OF_CACHEMEMORY;
		double threshold = Parameter.MEMORY_THRESHOLD;
		CacheMemoryManager cmm = new CacheMemoryManager(capacity, threshold);

		String[] diskPaths = Parameter.DISK_PATHS;
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
		String value = new String(byteVal); //TODO set character code
		return value;
	}


	public static void main(String[] args) throws IOException {
		int bufSize = 1024 * 1024 * 20;
		char[] buf = new char[bufSize];
		String bufValue = String.valueOf(buf);
		
		SimpleClient sc = new SimpleClient();
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
