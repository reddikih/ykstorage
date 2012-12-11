package jp.ac.titech.cs.de.ykstorage.service;

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

	public static final String[] DATA_DISK_PATHS = {
		"/ecoim/ykstorage/data/disk5/",
		"/ecoim/ykstorage/data/disk6/",
		"/ecoim/ykstorage/data/disk7/",
		"/ecoim/ykstorage/data/disk8/",
		"/ecoim/ykstorage/data/disk9/",
		"/ecoim/ykstorage/data/disk10/",
		"/ecoim/ykstorage/data/disk11/",
		"/ecoim/ykstorage/data/disk12/",
		"/ecoim/ykstorage/data/disk13/",
		"/ecoim/ykstorage/data/disk14/",
//		"/ecoim/ykstorage/data/disk15/",
//		"/ecoim/ykstorage/data/disk16/",
//		"/ecoim/ykstorage/data/disk17/",
//		"/ecoim/ykstorage/data/disk18/",
//		"/ecoim/ykstorage/data/disk19/",
//		"/ecoim/ykstorage/data/disk20/",
//		"/ecoim/ykstorage/data/disk21/",
//		"/ecoim/ykstorage/data/disk22/",
//		"/ecoim/ykstorage/data/disk23/",
//		"/ecoim/ykstorage/data/disk24/",
//		"/ecoim/ykstorage/data/disk25/",
//		"/ecoim/ykstorage/data/disk26/",
//		"/ecoim/ykstorage/data/disk27/",
//		"/ecoim/ykstorage/data/disk28/",
//		"/ecoim/ykstorage/data/disk29/",
//		"/ecoim/ykstorage/data/disk30/",
//		"/ecoim/ykstorage/data/disk31/",
//		"/ecoim/ykstorage/data/disk32/",
	};

	/**
	 * A number of data disks.
	 */
	public static final int NUMBER_OF_DATADISK = DATA_DISK_PATHS.length;

	public static final String[] CACHE_DISK_PATHS = {
		"/ecoim/ykstorage/data/disk1/",
		"/ecoim/ykstorage/data/disk2/",
		"/ecoim/ykstorage/data/disk3/",
		"/ecoim/ykstorage/data/disk4/",
	};

	public static final int NUMBER_OF_CACHEDISK = CACHE_DISK_PATHS.length;

	public static final String DATA_DISK_SAVE_FILE_PATH = "./datamap";

	/**
	 * Logger name of this system.
	 */
	public static final String LOGGER_NAME = "jp.ac.titech.cs.de.ykstorage.logger";

	public static final String LOG_DIR = System.getProperty("user.home");

	public static final String LOG_FILE_NAME = "ykstorage.log";

}
