package jp.ac.titech.cs.de.ykstorage.service;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import jp.ac.titech.cs.de.ykstorage.util.DiskState;
import jp.ac.titech.cs.de.ykstorage.util.StorageLogger;


// TODO MAIDDataDiskManagerの修正を適用する

public class DiskManager {
	private static final String SAVE_FILE_PATH = "file.map";
	
	private Logger logger = StorageLogger.getLogger();
	private StateManager sm;
	private String[] dataDiskPaths;
	private boolean persistence;
	
	// TODO dataDiskDevicesに変更
	/**
	 * key: disk path on file system
	 * value: device file path
	 */
	private SortedMap<String, String> mountPointPaths;

	/**
	 * key: data key
	 * value: actual file path corresponding to the key
	 */
	private HashMap<Integer, String> keyFileMap = new HashMap<Integer, String>();
	
	private int diskIndex = 0;	// ラウンドロビンでディスクの選択時に使用

	// TODO savePathを削除
	public DiskManager(
			String[] diskpaths,
			SortedMap<String, String> mountPointPaths,
			double spinDownThreshold,
			boolean persistence) {
		
		this.dataDiskPaths = diskpaths;
		this.mountPointPaths = mountPointPaths;
		this.persistence = persistence;

//		this.sm = new StateManager(this.mountPointPaths.values(), spinDownThreshold);
		ArrayList<String> dataDiskDevices = new ArrayList<String>();
		for(String diskpath : diskpaths) {
			dataDiskDevices.add(mountPointPaths.get(diskpath));
		}
		this.sm = new StateManager(dataDiskDevices, spinDownThreshold);

		init();
		this.sm.start();
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

	// TODO ワークロード終了時に必ず呼び出す
	public void end() {
		saveHashMap();
	}

	private void init() {
		for (String path : dataDiskPaths) {
			File f = new File(path);
			if(!f.exists() || !f.isDirectory()) {
				if(!f.mkdirs()) {
					throw new SecurityException("cannot create dir: " + path);
				}
				logger.fine("[MKDIR]: " + path);
			}
		}
		
		loadHashMap();
	}

	// TODO getDiskState() 
	// MAIDか提案手法で利用する?
	// 引数がdevicePathの方はいらなそう
	public DiskState getDiskState(String devicePath) {
		//String devicePath = mountPointPaths.get(diskPath);
		//return sm.getDiskState(devicePath);
		return sm.getDiskState(devicePath);
	}
	
	public DiskState getDiskState(int key) {
		String filePath = keyFileMap.get(key);
		String diskPath = getDiskPath(filePath);
		String devicePath = mountPointPaths.get(diskPath);
		return sm.getDiskState(devicePath);
	}
	
	private String getDiskPath(String filePath) {
		String diskPath = "";
		if(filePath != null) {
			diskPath = filePath.substring(0, filePath.lastIndexOf("/") + 1);
		}
		return diskPath;
	}

	private void loadHashMap() {
		try {
			File f = new File(SAVE_FILE_PATH);
			if(!f.isFile() || !persistence) {
				return;
			}
			BufferedReader br = new BufferedReader(new FileReader(f));

			String line = "";
			while((line = br.readLine()) != null) {
				StringTokenizer st = new StringTokenizer(line, ",");

				int key = Integer.parseInt(st.nextToken());
				String value = st.nextToken();
				keyFileMap.put(key, value);
			}

			br.close();
		}catch(FileNotFoundException e) {
			e.printStackTrace();
		}catch(IOException e) {
			e.printStackTrace();
		}
	}

	private void saveHashMap() {
		try {
			File f = new File(SAVE_FILE_PATH);
			if(!persistence) {
				f.deleteOnExit();
			}
			BufferedWriter bw = new BufferedWriter(new FileWriter(f));

			Iterator<Integer> keys = keyFileMap.keySet().iterator();
			while(keys.hasNext()) {
				int key = (Integer) keys.next();
				String value = keyFileMap.get(key);

				bw.write(key + "," + value);
				bw.newLine();
				bw.flush();
			}

			bw.close();
		}catch(FileNotFoundException e) {
			e.printStackTrace();
		}catch(IOException e) {
			e.printStackTrace();
		}
	}

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
		String filepath = dataDiskPaths[diskIndex] + key;
		diskIndex++;
		if(diskIndex > dataDiskPaths.length - 1) {
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
	
//	private boolean isStandby(int key) {
//		return getDiskState(key).equals(DiskState.STANDBY);
//	}
//
//	private boolean spinup(int key) {
//		return sm.spinup(getDevicePath(key));
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
			sm.setDiskState(devicePath, DiskState.ACTIVE);
			File f = new File(filepath);
			FileInputStream fis = new FileInputStream(f);
			BufferedInputStream bis = new BufferedInputStream(fis);

			byte[] value = new byte[(int) f.length()];
			bis.read(value);

			bis.close();
			result = new Value(value);
			logger.fine("[GET]: " + key + ", " + filepath + ", " + devicePath);
		}catch(Exception e) {
			e.printStackTrace();
			logger.warning("failed [GET]: " + key + ", " + filepath + ", " + devicePath);
		}finally {
			sm.setIdleIntime(devicePath, System.currentTimeMillis());
			sm.setDiskState(devicePath, DiskState.IDLE);
		}
		return result;
	}

	private boolean write(int key, Value value) {
		boolean result = false;
		
		String filepath = selectDisk(key);
//		int diskId = getDiskId(filepath);
		String diskPath = getDiskPath(filepath);
		String devicePath = mountPointPaths.get(diskPath);
		try {	
			sm.setDiskState(devicePath, DiskState.ACTIVE);
			File f = new File(filepath);
			if(!persistence) {
				f.deleteOnExit();
			}
			FileOutputStream fos = new FileOutputStream(f);
			BufferedOutputStream bos = new BufferedOutputStream(fos);

			bos.write(value.getValue());
			bos.flush();

			bos.close();
			result = true;
			logger.fine("[PUT]: " + key + ", " + filepath + ", " + devicePath);
		}catch(Exception e) {
			keyFileMap.remove(key);
			e.printStackTrace();
			logger.warning("failed [PUT]: " + key + ", " + filepath + ", " + devicePath);
		}finally {
			sm.setIdleIntime(devicePath, System.currentTimeMillis());
			sm.setDiskState(devicePath, DiskState.IDLE);
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
			sm.setDiskState(devicePath, DiskState.ACTIVE);
			File f = new File(filepath);
			result = f.delete();
			logger.fine("[DELETE]: " + key + ", " + filepath + ", " + devicePath);
		}catch(SecurityException e) {
			keyFileMap.put(key, filepath);
			e.printStackTrace();
			logger.warning("failed [DELETE]: " + key + ", " + filepath + ", " + devicePath);
		}finally {
			sm.setIdleIntime(devicePath, System.currentTimeMillis());
			sm.setDiskState(devicePath, DiskState.IDLE);
		}
		return result;
	}
}
