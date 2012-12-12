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
	public static final int CAPACITY_OF_CACHEMEMORY = 10;

	public static final double MEMORY_THRESHOLD = 1.0;

	/**
	 * The disk spin down threshold time(second).
	 */
	public static final double SPIN_DOWN_THRESHOLD = 10.0;

//	public static String DATA_DIR = "/ecoim/ykstorage/data";
	public static String DATA_DIR = "./data";

	public static String[] DATA_DISK_PATHS;
	static {
		int numOfDisks = 14;
		int origin = 1;
		String prefix = DATA_DIR + "/disk%d/";
		DATA_DISK_PATHS = new String[numOfDisks];
		for (int i=0; i < DATA_DISK_PATHS.length; i++) {
			DATA_DISK_PATHS[i] = String.format(prefix, origin + i);
		}
		// above code generate data disk paths like follows:
		//  /ecoim/ykstorage/data/disk1, /ecoim/ykstorage/data/disk2, ...
	};

	public static SortedMap<String, String> MOUNT_POINT_PATHS = new TreeMap<String, String>();
	static {
		char diskIds[] = {'b','c','d','e','f','g','h','i','j','k','l','m','n','o'};
		String prefix = "/dev/sd%s";
		int i = 0;
		for (char diskId : diskIds) {
			MOUNT_POINT_PATHS.put(DATA_DISK_PATHS[i], String.format(prefix, diskId));
			i++;
		}
		// above code generate data disk paths like follows:
		//  /dev/sdb, /dev/sdc, ...
	}

	/**
	 * A number of data disks.
	 */
	public static final int NUMBER_OF_DATA_DISKS = DATA_DISK_PATHS.length;

	public static final String[] CACHE_DISK_PATHS = {
		"/ecoim/ykstorage/data/disk1/",
		"/ecoim/ykstorage/data/disk2/",
		"/ecoim/ykstorage/data/disk3/",
		"/ecoim/ykstorage/data/disk4/",
	};

	public static final int NUMBER_OF_CACHE_DISKS = CACHE_DISK_PATHS.length;

	public static final String DATA_DISK_SAVE_FILE_PATH = "./datamap";

	/**
	 * Logger name of this system.
	 */
	public static final String LOGGER_NAME = "jp.ac.titech.cs.de.ykstorage.logger";

	public static final String LOG_DIR = System.getProperty("user.home");

	public static final String LOG_FILE_NAME = "ykstorage.log";

}
