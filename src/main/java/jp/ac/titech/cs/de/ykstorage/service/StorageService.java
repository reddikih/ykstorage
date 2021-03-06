package jp.ac.titech.cs.de.ykstorage.service;

import jp.ac.titech.cs.de.ykstorage.frontend.FrontEnd;
import jp.ac.titech.cs.de.ykstorage.storage.DiskManager;
import jp.ac.titech.cs.de.ykstorage.storage.OLDStorageManager;
import jp.ac.titech.cs.de.ykstorage.storage.StorageManager;
import jp.ac.titech.cs.de.ykstorage.storage.StorageManagerFactory;
import jp.ac.titech.cs.de.ykstorage.storage.buffer.CacheMemoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class StorageService {

    private static final Logger logger = LoggerFactory.getLogger(StorageService.class);

    private static final StorageService storageService = new StorageService();

    private FrontEnd frontend;

    @Deprecated
    private OLDStorageManager storageManager = null; //TODO delete in near future

    private void initialize(String[] args) {
        Parameter parameter = getStorageParameter(args[0]);

        StorageManagerFactory smFactory =
                StorageManagerFactory.getStorageManagerFactory(parameter);

        StorageManager storageManager = smFactory.createStorageManager();

        logger.debug("Create storage manager:{}", storageManager.getClass().getSimpleName());

        // Generate FrontEnd and start listening the client requests.
        try {
            this.frontend = FrontEnd.getInstance(parameter.serverPort, storageManager, storageService);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Couldn't create FrontEnd server.");
            System.exit(1);
        }

    }

    private Parameter getStorageParameter(String configPath) {
        return new Parameter(configPath);
    }

    private void start() {
        if (this.frontend == null) {
            String error = "FrontEnd is not available.";
            logger.error(error);
            throw new IllegalAccessError(error);
        }

        // TODO consider termination for robust shutdown.
        this.frontend.start();
    }

    @Deprecated
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

		this.storageManager = new OLDStorageManager(cmm, dm);

	}

	public boolean put(String key, String value) {
		byte[] byteVal = value.getBytes();
		return storageManager.put(key, byteVal);
	}

	public Object get(String key) {
		byte[] byteVal = storageManager.get(key);
		String value = new String(byteVal); //TODO set character code
		return value;
	}

    public static void main(String[] args) {
        storageService.initialize(args);

        storageService.start();
   }

    public void exit() {
        System.out.println("-----------------------------------");
        System.out.println("Storage Service is terminated. Bye.");
        System.exit(0);
    }
}
