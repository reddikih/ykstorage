package jp.ac.titech.cs.de.ykstorage.service;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashMap;


public class DiskManager {
	private String[] diskpaths;
	
	private HashMap<Integer, String> keyFileMap = new HashMap<Integer, String>();
	private int diskIndex = 0;	// ラウンドロビンでディスクの選択時に使用
	
	
	public DiskManager() {
		this.diskpaths = new String[2];
		this.diskpaths[0] = "/disk0/";
		this.diskpaths[1] = "/disk1/";
	}
	
	public DiskManager(String[] diskpaths) {
		this.diskpaths = diskpaths;
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
				return diskId;
			}
		}
		return -1;
	}
	
	private Value read(int key) {
		String filepath = keyFileMap.get(key);
		if(filepath == null) {
			return Value.NULL;
		}
		try {
			File f = new File(filepath);
			FileInputStream fis = new FileInputStream(f);
			BufferedInputStream bis = new BufferedInputStream(fis);
			
			byte[] value = new byte[(int) f.length()];
			bis.read(value);
			
			bis.close();
			return new Value(value);
		}catch(Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private boolean write(int key, Value value) {
		String filepath = selectDisk(key);
		try {
			File f = new File(filepath);
			FileOutputStream fos = new FileOutputStream(f);
			BufferedOutputStream bos = new BufferedOutputStream(fos);
			
			bos.write(value.getValue());
			bos.flush();
			
			bos.close();
			return true;
		}catch(Exception e) {
			keyFileMap.remove(key);
			e.printStackTrace();
		}
		return false;
	}
	
	private boolean remove(int key) {
		String filepath = keyFileMap.get(key);
		if(filepath == null) {
			return false;
		}
		keyFileMap.remove(key);
		try {
			File f = new File(filepath);
			return f.delete();
		}catch(SecurityException e) {
			keyFileMap.put(key, filepath);
			e.printStackTrace();
		}
		return false;
	}
}
