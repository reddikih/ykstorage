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
import java.util.HashMap;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.StringTokenizer;

import jp.ac.titech.cs.de.ykstorage.util.DiskState;


public class DiskManager {
	private StateManager sm;

	private String[] diskpaths;
	private String savePath;
	/**
	 * key: disk path on file system
	 * value: device file path
	 */
	private SortedMap<String, String> mountPointPaths;

	private HashMap<Integer, String> keyFileMap = new HashMap<Integer, String>();
	private int diskIndex = 0;	// ラウンドロビンでディスクの選択時に使用

	public DiskManager(
			String[] diskpaths,
			String savePath,
			SortedMap<String, String> mountPointPaths,
			double spinDownThreshold) {
		this.diskpaths = diskpaths;
		this.savePath = savePath;
		this.mountPointPaths = mountPointPaths;

		this.sm = new StateManager(this.mountPointPaths.keySet(), spinDownThreshold);

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

	public void end() {
		saveHashMap();
	}

	private void init() {
		for (String path : diskpaths) {
			File f = new File(path);
			if(!f.exists() || !f.isDirectory()) {
				if(!f.mkdirs()) {
					throw new SecurityException("cannot create dir: " + path);
				}
			}
		}

		loadHashMap();
	}

	public DiskState getDiskState(int diskId) {
		return sm.getDiskState(diskId);
	}

	private void loadHashMap() {
		try {
			File f = new File(savePath);
			if(!f.isFile()) {
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
			File f = new File(savePath);
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
		String filepath = diskpaths[diskIndex] + key;
		diskIndex++;
		if(diskIndex > diskpaths.length - 1) {
			diskIndex = 0;
		}
		return filepath;
	}

	private int getDiskId(String filepath) {
		int diskId = -1;
		for(int i = 0; i < diskpaths.length; i++) {
			diskId = filepath.indexOf(diskpaths[i]);
			if(diskId > -1) {
				return diskId + 1;
			}
		}
		return -1;
	}

	private Value read(int key) {
		String filepath = keyFileMap.get(key);
		if(filepath == null) {
			return Value.NULL;
		}

		int diskId = getDiskId(filepath);
		try {
			sm.setDiskState(diskId, DiskState.ACTIVE);
			File f = new File(filepath);
			FileInputStream fis = new FileInputStream(f);
			BufferedInputStream bis = new BufferedInputStream(fis);

			byte[] value = new byte[(int) f.length()];
			bis.read(value);

			bis.close();
			sm.setIdleIntime(diskId, System.currentTimeMillis());
			sm.setDiskState(diskId, DiskState.IDLE);
			return new Value(value);
		}catch(Exception e) {
			e.printStackTrace();
		}
		sm.setIdleIntime(diskId, System.currentTimeMillis());
		sm.setDiskState(diskId, DiskState.IDLE);
		return Value.NULL;
	}

	private boolean write(int key, Value value) {
		String filepath = selectDisk(key);
		int diskId = getDiskId(filepath);
		try {
			sm.setDiskState(diskId, DiskState.ACTIVE);
			File f = new File(filepath);
			if(Parameter.DEBUG) {
				f.deleteOnExit();
			}
			FileOutputStream fos = new FileOutputStream(f);
			BufferedOutputStream bos = new BufferedOutputStream(fos);

			bos.write(value.getValue());
			bos.flush();

			bos.close();
			sm.setIdleIntime(diskId, System.currentTimeMillis());
			sm.setDiskState(diskId, DiskState.IDLE);
			return true;
		}catch(Exception e) {
			keyFileMap.remove(key);
			e.printStackTrace();
		}
		sm.setIdleIntime(diskId, System.currentTimeMillis());
		sm.setDiskState(diskId, DiskState.IDLE);
		return false;
	}

	private boolean remove(int key) {
		String filepath = keyFileMap.get(key);
		if(filepath == null) {
			return false;
		}
		keyFileMap.remove(key);

		int diskId = getDiskId(filepath);
		try {
			sm.setDiskState(diskId, DiskState.ACTIVE);
			File f = new File(filepath);
			boolean ret = f.delete();
			sm.setIdleIntime(diskId, System.currentTimeMillis());
			sm.setDiskState(diskId, DiskState.IDLE);
			return ret;
		}catch(SecurityException e) {
			keyFileMap.put(key, filepath);
			e.printStackTrace();
		}
		sm.setIdleIntime(diskId, System.currentTimeMillis());
		sm.setDiskState(diskId, DiskState.IDLE);
		return false;
	}
}
