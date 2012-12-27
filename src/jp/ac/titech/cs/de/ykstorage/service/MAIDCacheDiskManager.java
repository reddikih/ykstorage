package jp.ac.titech.cs.de.ykstorage.service;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.SortedMap;
import java.util.logging.Logger;

import jp.ac.titech.cs.de.ykstorage.util.StorageLogger;


public class MAIDCacheDiskManager {
	private Logger logger = StorageLogger.getLogger();
//	private StateManager sm;

	private String[] diskpaths;
//	private String savePath;
	private long maxCapacity;
	private HashMap<String, Long> capacity = new HashMap<String, Long>();
	
	/**
	 * key: disk path on file system
	 * value: device file path
	 */
	private SortedMap<String, String> mountPointPaths;

	/**
	 * key: data key
	 * value: actual file path corresponding to the key
	 */
//	private HashMap<Integer, String> keyFileMap = new HashMap<Integer, String>();
	private LinkedHashMap<Integer, String> keyFileMap = new LinkedHashMap<Integer, String>();
	
	private int diskIndex = 0;	// ラウンドロビンでディスクの選択時に使用

	public MAIDCacheDiskManager(
			String[] diskpaths,
			String savePath,
			SortedMap<String, String> mountPointPaths,
			double spinDownThreshold,
			long maxCapacity) {
		this.diskpaths = diskpaths;
//		this.savePath = savePath;
		this.mountPointPaths = mountPointPaths;
		this.maxCapacity = maxCapacity;
//		this.sm = new StateManager(this.mountPointPaths.values(), spinDownThreshold);
//
		init();
//		this.sm.start();
	}

	public Value get(int key) {
		return read(key);
	}

	public boolean put(int key, Value value) {
		return write(key, value);
	}

	public boolean delete(int key) {
		return remove(key);
	}

//	public void end() {
//		saveHashMap();
//	}

	private void init() {
		for (String path : diskpaths) {
			File f = new File(path);
			if(!f.exists() || !f.isDirectory()) {
				if(!f.mkdirs()) {
					throw new SecurityException("cannot create dir: " + path);
				}
			}
			String devicePath = mountPointPaths.get(path);
			capacity.put(devicePath, 0L);
		}

//		loadHashMap();
	}

//	public DiskState getDiskState(String devicePath) {
//		//String devicePath = mountPointPaths.get(diskPath);
//		//return sm.getDiskState(devicePath);
//		return sm.getDiskState(devicePath);
//	}
	
//	public DiskState getDiskState(int key) {
//		String filePath = keyFileMap.get(key);
//		String diskPath = getDiskPath(filePath);
//		String devicePath = mountPointPaths.get(diskPath);
//		return sm.getDiskState(devicePath);
//	}
	
	private String getDiskPath(String filePath) {
		String diskPath = "";
		if(filePath != null) {
			diskPath = filePath.substring(0, filePath.lastIndexOf("/") + 1);
		}
		return diskPath;
	}

//	private void loadHashMap() {
//		try {
//			File f = new File(savePath);
//			if(!f.isFile()) {
//				return;
//			}
//			BufferedReader br = new BufferedReader(new FileReader(f));
//
//			String line = "";
//			while((line = br.readLine()) != null) {
//				StringTokenizer st = new StringTokenizer(line, ",");
//
//				int key = Integer.parseInt(st.nextToken());
//				String value = st.nextToken();
//				keyFileMap.put(key, value);
//			}
//
//			br.close();
//		}catch(FileNotFoundException e) {
//			e.printStackTrace();
//		}catch(IOException e) {
//			e.printStackTrace();
//		}
//	}

//	private void saveHashMap() {
//		try {
//			File f = new File(savePath);
//			BufferedWriter bw = new BufferedWriter(new FileWriter(f));
//
//			Iterator<Integer> keys = keyFileMap.keySet().iterator();
//			while(keys.hasNext()) {
//				int key = (Integer) keys.next();
//				String value = keyFileMap.get(key);
//
//				bw.write(key + "," + value);
//				bw.newLine();
//				bw.flush();
//			}
//
//			bw.close();
//		}catch(FileNotFoundException e) {
//			e.printStackTrace();
//		}catch(IOException e) {
//			e.printStackTrace();
//		}
//	}

	// キーに基づいて格納先のディスクを選択
	private String selectDisk(int key) {
		String filepath = keyFileMap.get(key);
		if(filepath != null) {
			return filepath;
		}

		filepath = roundRobin(key);
		keyFileMap.put(key, filepath);
		return filepath;
	}

	private String roundRobin(int key) {
		String filepath = diskpaths[diskIndex] + key;
		diskIndex++;
		if(diskIndex > diskpaths.length - 1) {
			diskIndex = 0;
		}
		return filepath;
	}

//	private int getDiskId(String filepath) {
//		int diskId = -1;
//		for(int i = 0; i < diskpaths.length; i++) {
//			diskId = filepath.indexOf(diskpaths[i]);
//			if(diskId > -1) {
//				return diskId + 1;
//			}
//		}
//		return -1;
//	}

	private Value read(int key) {
		Value result = Value.NULL;
		
		String filepath = keyFileMap.get(key);
		if(filepath == null) {
			return result;
		}
//		int diskId = getDiskId(filepath);
		String diskPath = getDiskPath(filepath);
		String devicePath = mountPointPaths.get(diskPath);
		try {
//			sm.setDiskState(devicePath, DiskState.ACTIVE);
			File f = new File(filepath);
			FileInputStream fis = new FileInputStream(f);
			BufferedInputStream bis = new BufferedInputStream(fis);

			byte[] value = new byte[(int) f.length()];
			bis.read(value);

			bis.close();
			result = new Value(value);
			
			keyFileMap.put(key, keyFileMap.remove(key));
			logger.fine("CacheDisk [GET]: " + key + ", " + filepath + ", " + devicePath);
		}catch(Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	private boolean write(int key, Value value) {
		boolean result = false;
		long valueSize = value.getValue().length;
		
		String filepath = selectDisk(key);
//		int diskId = getDiskId(filepath);
		String diskPath = getDiskPath(filepath);
		String devicePath = mountPointPaths.get(diskPath);
		
		if(valueSize > maxCapacity) {
			return false;
		}
		while(capacity.get(devicePath) + valueSize > maxCapacity) {
			if(!lru(devicePath)) {
				return false;
			}
		}
		
		try {	
//			sm.setDiskState(devicePath, DiskState.ACTIVE);
			File f = new File(filepath);
			if(Parameter.DEBUG) {
				f.deleteOnExit();
			}
			FileOutputStream fos = new FileOutputStream(f);
			BufferedOutputStream bos = new BufferedOutputStream(fos);

			bos.write(value.getValue());
			bos.flush();

			bos.close();
			
//			capacity += valueSize;
			capacity.put(devicePath, capacity.get(devicePath) + valueSize);
			result = true;
			
			keyFileMap.put(key, keyFileMap.remove(key));
			logger.fine("CacheDisk [PUT]: " + key + ", " + filepath + ", " + devicePath);
		}catch(Exception e) {
			keyFileMap.remove(key);
			e.printStackTrace();
		}
		return result;
	}

	private boolean remove(int key) {
		boolean result = false;
		
		String filepath = keyFileMap.get(key);
		if(filepath == null) {
			return result;
		}
		
		keyFileMap.remove(key);
//		int diskId = getDiskId(filepath);
		//String devicePath = mountPointPaths.get(selectDisk(key));
		String diskPath = getDiskPath(filepath);
		String devicePath = mountPointPaths.get(diskPath);
		try {
//			sm.setDiskState(devicePath, DiskState.ACTIVE);
			File f = new File(filepath);
			long tmp = f.length();
			result = f.delete();
			
//			capacity -= tmp;
			capacity.put(devicePath, capacity.get(devicePath) - tmp);
			logger.fine("CacheDisk [DELETE]: " + key + ", " + filepath + ", " + devicePath);
		}catch(SecurityException e) {
			keyFileMap.put(key, filepath);
			e.printStackTrace();
		}
		return result;
	}
	
	private boolean lru(String devicePath) {
		// アクセスが古い順にremoveする
		Iterator<Integer> itr = keyFileMap.keySet().iterator();
		while(itr.hasNext()) {
			int key = itr.next();
			// key が devicePath かどうかの確認
			String filePath = keyFileMap.get(key);
			String diskPath = getDiskPath(filePath);
			if(devicePath.equals(mountPointPaths.get(diskPath))) {
				logger.fine("CacheDisk [LRU]: " + keyFileMap.get(key));
				return remove(key);
			}			
		}
		return false;
	}
}
