package jp.ac.titech.cs.de.ykstorage.service;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    private static Properties config;

    public Parameter(String configPath) {

        this.config = new Properties(System.getProperties());

        if (configPath != null)
            try {
                config.load(new BufferedInputStream(new FileInputStream(configPath)));
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }

        initialize();
    }

    private void initialize() {
        Parameter.BLOCK_SIZE = (int)convertSizeParameter(config.getProperty("block.size"));
        this.storageManagerFactory = config.getProperty("storage.manager.factory");
        this.numberOfBuffers = Integer.parseInt(config.getProperty("buffer.number"));
        this.numberOfCacheDisks = Integer.parseInt(config.getProperty("cachedisk.number"));
        this.numberOfDataDisks = Integer.parseInt(config.getProperty("datadisk.number"));
        this.spindownThresholdTime = Double.parseDouble(config.getProperty("spindown.threshold.time"));
        this.serverPort = Integer.parseInt(config.getProperty("server.port"));
        this.driveCharacters = config.getProperty("device.characters").split(",");
        this.ykstorageHome = config.getProperty("ykstorage.home");
        this.dataDir = config.getProperty("data.dir");
        this.diskFilePathPrefix = concatenatePathStrings(ykstorageHome, concatenatePathStrings(dataDir, config.getProperty("disk.file.prefix")));
        this.devicePathPrefix = config.getProperty("device.file.prefix");
        this.bufferCapacity = convertSizeParameter(config.getProperty("buffer.size"));
        this.bufferWaterMark = Double.parseDouble(config.getProperty("buffer.threshold"));
    }

    private String concatenatePathStrings(String parent, String child) {
        if (parent == null || child == null)
            return null;

        if (parent.endsWith("/")) parent = parent.substring(parent.length() - 2);
        if (child.startsWith("/")) child = child.substring(1);

        return parent + "/" + child;
    }

    private long convertSizeParameter(String sizeStr) {
        long result = 1L;
        sizeStr = sizeStr.toLowerCase();
        Matcher m = Pattern.compile("(?<number>[1-9][0-9]*)(?<unit>k|m|g|t)?b?").matcher(sizeStr);
        if (m.matches()) {
            if(m.group("unit") != null) {
                switch(m.group("unit")) {
                    case "t": result *= 1024;
                    case "g": result *= 1024;
                    case "m": result *= 1024;
                    case "k": result *= 1024;
                    default: break;
                }
            }
        } else {
            throw new IllegalArgumentException("the format of this parameter is invalid: " + sizeStr);
        }
        return result * Long.parseLong(m.group("number"));
    }


    //--- these are the parameters ---//

    /** Debug flag */
	public static final boolean DEBUG = true;

	/** Capacity of cache memory. It's unit is byte. */
    @Deprecated
	public static final int CAPACITY_OF_CACHEMEMORY = 64 * 1024 * 1024;

    /** Capacity of cache memory. It's unit is byte. */
    @Deprecated
    public static final long CAPACITY_OF_CACHEDISK = 64 * 1024 * 1024;

    @Deprecated
    public static final double MEMORY_THRESHOLD = 1.0;

    @Deprecated
	/** The disk spin down threshold time(second). */
	public static final double SPIN_DOWN_THRESHOLD = 10.0;

    @Deprecated
    public static String YKSTORAGE_HOME = System.getProperty("user.dir");

    @Deprecated
    public static final String DATA_DIR =YKSTORAGE_HOME + "/data";

    @Deprecated
    public static final String LOG_DIR =YKSTORAGE_HOME + "/log";

    @Deprecated
    public static final String DISK_PREFIX = "/disk%d";

    @Deprecated
    public static final String DEVICE_PREFIX = "/dev/sd%s";

    @Deprecated
	public static int NUMBER_OF_CACHE_DISKS = 2;

    @Deprecated
	public static int NUMBER_OF_DATA_DISKS = 18;

    @Deprecated
    public static int NUMBER_OF_DISKS = NUMBER_OF_CACHE_DISKS + NUMBER_OF_DATA_DISKS;

    private static final int ORIGIN = 1;

    public static int NUMBER_OF_DISKS_PER_CACHE_GROUP = 2;

    @Deprecated
    public static int NUMBER_OF_REPLICA = 3;

    public int numberOfBuffers;

    public int numberOfCacheDisks;

    public int numberOfDataDisks;

    public double spindownThresholdTime ;

    /**
     * This value is one of them:
     * NormalStorageManagerFactory, MAIDStorageManagerFactory, RAPoSDAStorageManagerFactory
     */
    public String storageManagerFactory;

    /** cs, dga, random  */
    public String bufferAllocationPolicy = "cs";

    public String bufferManagerFactory = "NormalBufferManager";

    public long bufferCapacity;

    public double bufferWaterMark;

    public static int BLOCK_SIZE = 32 * 1024;

    public int serverPort;

    public String[] driveCharacters;

    public String ykstorageHome;

    public String dataDir;

    /**
     * prefix string of disk file paths.
     *
     * ${YKSTORAGE_HOME}/${DATA_DIR}/sd
     * ex) /var/ykstorage/data/sd
     */
    public String diskFilePathPrefix;

    public String devicePathPrefix;



    /*--- These are settigs for Oguri kun ---*/

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
