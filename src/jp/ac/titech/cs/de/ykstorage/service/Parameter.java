package jp.ac.titech.cs.de.ykstorage.service;

import java.util.SortedMap;
import java.util.TreeMap;

/**
 * 起動パラメータ用の一時的なクラス．最終的にはXMLやプロパティファイル
 * から起動時にパラメータを読み込むようにさせる．
 * 出来ればコマンドライン引数でも指定出来るようにし，コマンドライン引数
 * で指定されたパラメータはその値が優先的に使われるようにしたい．
 *
 * @author hikida
 *
 */
public class Parameter {

	/**
	 * Debug flag
	 */
	public static final boolean DEBUG = true;

	/**
	 * Capacity of cache memory. It's unit is byte.
	 */
	public static final int CAPACITY_OF_CACHEMEMORY = 64 * 1024 * 1024;
//	public static final int CAPACITY_OF_CACHEMEMORY = 0;

	public static final double MEMORY_THRESHOLD = 1.0;

	/**
	 * The disk spin down threshold time(second).
	 */
	public static final double SPIN_DOWN_THRESHOLD = 10.0;
//	public static final double SPIN_DOWN_THRESHOLD = 1.0;
	
	/**
	 * Capacity of cache memory. It's unit is byte.
	 */
	public static final long CAPACITY_OF_CACHEDISK = 64 * 1024 * 1024;
//	public static final long CAPACITY_OF_CACHEDISK = 10;

//	public static String DATA_DIR = "/ecoim/ykstorage/data";
	public static String DATA_DIR = "./data";

	public static final int NUMBER_OF_DISKS = 14;
	private static final int origin = 1;
	public static final int NUMBER_OF_CACHE_DISKS = 5;
	public static final int NUMBER_OF_DATA_DISKS = NUMBER_OF_DISKS - NUMBER_OF_CACHE_DISKS;
	private static final char diskIds[] = {'b','c','d','e','f','g','h','i','j','k','l','m','n','o',
										   'p','q','r','s','t','u','v','w','x','y','z'};
	
	public static String[] DISK_PATHS;
	static {
//		int numOfDisks = 14;
//		int origin = 1;
		String prefix = DATA_DIR + "/disk%d/";
		DISK_PATHS = new String[NUMBER_OF_DISKS];
		for (int i=0; i < DISK_PATHS.length; i++) {
			DISK_PATHS[i] = String.format(prefix, origin + i);
		}
		// above code generate data disk paths like follows:
		//  /ecoim/ykstorage/data/disk1/, /ecoim/ykstorage/data/disk2/, ...
	};
	
	public static String[] DATA_DISK_PATHS;
	static {
//		int numOfDisks = 10;
//		int origin = 5;
		int numOfDataDisks = NUMBER_OF_DISKS - NUMBER_OF_CACHE_DISKS;
		int originOfDataDisks = origin + NUMBER_OF_CACHE_DISKS;
		String prefix = DATA_DIR + "/disk%d/";
		DATA_DISK_PATHS = new String[numOfDataDisks];
		for (int i=0; i < DATA_DISK_PATHS.length; i++) {
			DATA_DISK_PATHS[i] = String.format(prefix, originOfDataDisks + i);
		}
		// above code generate data disk paths like follows:
		//  /ecoim/ykstorage/data/disk1/, /ecoim/ykstorage/data/disk2/, ...
	};
	
	public static String[] CACHE_DISK_PATHS;
	static {
//		int numOfDisks = 4;
//		int origin = 1;
		String prefix = DATA_DIR + "/disk%d/";
		CACHE_DISK_PATHS = new String[NUMBER_OF_CACHE_DISKS];
		for (int i=0; i < CACHE_DISK_PATHS.length; i++) {
			CACHE_DISK_PATHS[i] = String.format(prefix, origin + i);
		}
		// above code generate data disk paths like follows:
		//  /ecoim/ykstorage/data/disk1/, /ecoim/ykstorage/data/disk2/, ...
	};

	public static SortedMap<String, String> MOUNT_POINT_PATHS = new TreeMap<String, String>();
	static {
//		char diskIds[] = {'b','c','d','e','f','g','h','i','j','k','l','m','n','o'};
		char[] ids = new char[NUMBER_OF_DISKS];
		for(int i = 0; i < ids.length; i++) {
			ids[i] = diskIds[i + origin - 1];
		}
		String prefix = "/dev/sd%s";
		int i = 0;
		for (char diskId : ids) {
			MOUNT_POINT_PATHS.put(DISK_PATHS[i], String.format(prefix, diskId));
			i++;
		}
		// above code generate data disk paths like follows:
		//  /dev/sdb, /dev/sdc, ...
	}

	public static final String DATA_DISK_SAVE_FILE_PATH = "./datamap";

	/**
	 * Logger name of this system.
	 */
	public static final String LOGGER_NAME = "jp.ac.titech.cs.de.ykstorage.logger";

//	public static final String LOG_DIR = System.getProperty("user.home");
//	public static final String LOG_DIR = "/ecoim/ykstorage";
	public static String LOG_DIR;
	static {
		if(DATA_DIR.equals("./data")) {
			LOG_DIR = System.getProperty("user.home");
		} else {
			LOG_DIR = "/ecoim/ykstorage";
		}
	}

	public static final String LOG_FILE_NAME = "ykstorage.log";
	
	public static final boolean DISKMANAGER_SPINDOWN = true;
//	public static final boolean DISKMANAGER_SPINDOWN = false;
	
//	public static final boolean PROPOSAL1 = true;
	public static final boolean PROPOSAL1 = false;
	
//	public static final boolean PROPOSAL2 = true;
	public static final boolean PROPOSAL2 = false;
	
	/**
	 * for Replace method
	 */
	public static final int NUMBER_OF_DATA = 1000;
	public static final double RE_ACCESS_THRESHOLD = 1.0;
	public static final long RE_INTERVAL = 10000;
	
	/**
	 * StreamSpinnerの稼働するマシン名を指定
	 */
	public static final String RMI_URL = "rmi://localhost/StreamSpinnerServer";
//	public static final String RMI_URL = "rmi://192.168.172.130/StreamSpinnerServer";
	
	/**
	 * 各チャンネルがCacheDiskかどうか
	 */
	public static boolean[] IS_CACHEDISK;
	static {
		IS_CACHEDISK = new boolean[NUMBER_OF_DISKS];
		for(int i = 0; i < IS_CACHEDISK.length; i++) {
			if(i < NUMBER_OF_CACHE_DISKS) {	// 始めのチャンネルをCacheDiskにしている
				IS_CACHEDISK[i] = true;
			} else {
				IS_CACHEDISK[i] = false;
			}
		}
	}
	
	/**
	 * the number of accesses per second.
	 */
	public static final double ACCESS_THRESHOLD = 1.0;
	
	public static final long ACCESS_INTERVAL = 10000;
	
	public static final long SPINDOWN_INTERVAL = 10000;
	
	public static final double ACC = 0.5;
	
	public static final long MEMORYHILOGGER_INTERVAL = 10;
	
}
