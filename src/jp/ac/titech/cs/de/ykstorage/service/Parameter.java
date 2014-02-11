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

	/** Debug flag */
	public static final boolean DEBUG = true;

	/** Capacity of cache memory. It's unit is byte. */
	public static final int CAPACITY_OF_CACHEMEMORY = 64 * 1024 * 1024;

    /** Capacity of cache memory. It's unit is byte. */
    public static final long CAPACITY_OF_CACHEDISK = 64 * 1024 * 1024;

    public static final double MEMORY_THRESHOLD = 1.0;

	/** The disk spin down threshold time(second). */
	public static final double SPIN_DOWN_THRESHOLD = 10.0;

    public static String YKSTORAGE_HOME = System.getProperty("user.dir");

    public static final String DATA_DIR =YKSTORAGE_HOME + "/data";

    public static final String LOG_DIR =YKSTORAGE_HOME + "/log";

    public static final String DISK_PREFIX = "/disk%d";

    public static final String DEVICE_PREFIX = "/dev/sd%s";

	public static int NUMBER_OF_CACHE_DISKS = 2;

	public static int NUMBER_OF_DATA_DISKS = 18;

    public static int NUMBER_OF_DISKS = NUMBER_OF_CACHE_DISKS + NUMBER_OF_DATA_DISKS;

    private static final int ORIGIN = 1;

    public static int NUMBER_OF_DISKS_PER_CACHE_GROUP = 2;

    public static int NUMBER_OF_REPLICA = 3;

    public String STORAGE_MANAGER_FACTORY_NAME = "NormalStorageManagerFactory";

    /** cs, dga, random  */
    public String BUFFER_ALLOCATION_POLICY = "cs";

    public String BUFFER_MANAGER_FACTORY = "NormalBufferManager";

    public static int BLOCK_SIZE = 32 * 1024 * 1024;

    public int serverPort = 9999;

    public char[] driveCharacters = {
            'b','c','d','e','f','g','h','i','j','k',
            'l','m','n','o','p','q','r','s','t','u',
            'v','w','x','y','z',
    };

    /**
     * ${YKSTORAGE_HOME}/${DATA_DIR}/${DISK_PREFIX}${i}
     * ex) /var/ykstorage/data/diska, /var/ykstorage/data/diskb, ...
     */
    public String[] diskFilePaths;

    /**
     * subset of diskFilePaths
     *
     * ${YKSTORAGE_HOME}/${DATA_DIR}/${DISK_PREFIX}${i}
     * ex) /var/ykstorage/data/diska, /var/ykstorage/data/diskb, ...
     */
    public String[] dataDiskPaths;

    /**
     * subset of diskFilePaths
     *
     * ${YKSTORAGE_HOME}/${DATA_DIR}/${DISK_PREFIX}${i}
     * ex) /var/ykstorage/data/diska, /var/ykstorage/data/diskb, ...
     */
    public String[] cacheDiskPaths;


    @Deprecated
    private static final char diskIds[] = {
            'b','c','d','e','f','g','h','i','j','k',
            'l','m','n','o','p','q','r','s','t','u',
            'v','w','x','y','z',
    };

    @Deprecated
	public static String[] DISK_PATHS;
	static {
		DISK_PATHS = new String[NUMBER_OF_DISKS];
		for (int i=0; i < DISK_PATHS.length; i++) {
			DISK_PATHS[i] = String.format(DATA_DIR + DISK_PREFIX, ORIGIN + i);
		}
	};

    @Deprecated
	public static String[] DATA_DISK_PATHS;
	static {
		DATA_DISK_PATHS = new String[NUMBER_OF_DISKS - NUMBER_OF_CACHE_DISKS];
		for (int i=0; i < DATA_DISK_PATHS.length; i++) {
			DATA_DISK_PATHS[i] = String.format(
                    DATA_DIR + DISK_PREFIX, ORIGIN + NUMBER_OF_CACHE_DISKS + i);
		}
	};

    @Deprecated
	public static String[] CACHE_DISK_PATHS;
	static {
		CACHE_DISK_PATHS = new String[NUMBER_OF_CACHE_DISKS];
		for (int i=0; i < CACHE_DISK_PATHS.length; i++) {
			CACHE_DISK_PATHS[i] = String.format(DATA_DIR + DISK_PREFIX, ORIGIN + i);
		}
	};

    @Deprecated
	public static SortedMap<String, String> MOUNT_POINT_PATHS = new TreeMap<String, String>();
	static {
		char[] ids = new char[NUMBER_OF_DISKS];
		for(int i = 0; i < ids.length; i++) {
			ids[i] = diskIds[i + ORIGIN - 1];
		}
		int i = 0;
		for (char diskId : ids) {
			MOUNT_POINT_PATHS.put(DISK_PATHS[i], String.format(DEVICE_PREFIX, diskId));
			i++;
		}
	}


    /*--- These are settigs for Oguri kun ---*/
	public static final String DATA_DISK_SAVE_FILE_PATH = "./datamap";

	public static final boolean PROPOSAL1 = false;
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

    /*--- this should be delete in the future ---*/
    public static final boolean DISKMANAGER_SPINDOWN = true;
}
