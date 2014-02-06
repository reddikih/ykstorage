package jp.ac.titech.cs.de.ykstorage.cli;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Logger;

import jp.ac.titech.cs.de.ykstorage.storage.DiskManager;
import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.StorageManager;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.CacheMemoryManager;
import jp.ac.titech.cs.de.ykstorage.util.StorageLogger;


public class SimpleClient2 {
	private static Logger logger = StorageLogger.getLogger();
	
	private static final int cmdIndex = 0;
	private static final int intervalIndex = 1;
	private static final int keylIndex = 2;
	private static final int valuelIndex = 3;
	
	static private long responseTime = 0L;
	
	private StorageManager sm;

	public SimpleClient2() {
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
		String value = new String(byteVal);
		return value;
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		int bufSize = 1024 * 1024 * 20;
		char[] buf = new char[bufSize];
		String bufValue = String.valueOf(buf);
		
		SimpleClient2 sc = new SimpleClient2();
		
//		File f = new File(WORKLOAD_PATH);
		File f = new File(args[0]);
		FileReader fr = new FileReader(f);
		BufferedReader br = new BufferedReader(fr);
		String line = "";
		int interval = 0;
		
		long startTime = 0L;
		long endTime = 0L;
		
		logger.fine("MAIDSimpleClient [START]: " + System.currentTimeMillis());
		int i = 0;
		while((line = br.readLine()) != null) {
			interval = 0;
			String[] cmdArray = line.split(",");
			
			i++;
			System.out.print(i + " ");
			startTime = System.currentTimeMillis();
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
			
			endTime = System.currentTimeMillis();
			
			responseTime += endTime - startTime;
			
			Thread.sleep(interval);
		}
		
		logger.fine("[Access] response time(millisecond): " + responseTime);
		br.close();
		
		System.out.println("finished");
		logger.fine("MAIDSimpleClient [END]: " + System.currentTimeMillis());
	}

}
