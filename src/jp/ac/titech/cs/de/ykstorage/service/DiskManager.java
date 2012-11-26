package jp.ac.titech.cs.de.ykstorage.service;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashMap;


public class DiskManager {
	private static final String[] diskpaths = {"/disk0/", "/disk1/"};	// TODO
	
	private HashMap<String, String> keyFileMap = new HashMap<String, String>();
	private int diskIndex = 0;	// ラウンドロビンでディスクの選択時に使用
	
	
	public byte[] get(String key) {
		return isNullOrEmpty(key) ? null: read(key); 
	}
	
	public boolean put(String key, byte[] value) {
		return isNullOrEmpty(key) ? false: write(key, value);
	}
	
	public boolean delete(String key) {
		return isNullOrEmpty(key) ? false: remove(key);
	}
	
	private static boolean isNullOrEmpty(String key) {
		if(key == null || key.isEmpty()) {
			return true;
		}
		return false;
	}
	
	// キーに基づいて格納先のディスクを選択
	private String selectDisk(String key) {
		String filepath = keyFileMap.get(key);
		if(filepath != null) {
			return filepath;
		}
		
		filepath = roundRobin(key);
		keyFileMap.put(key, filepath);
		return filepath;
	}
	
	private String roundRobin(String key) {
		String filepath = diskpaths[diskIndex] + key;
		diskIndex++;
		if(diskIndex > diskpaths.length - 1) {
			diskIndex = 0;
		}
		return filepath;
	}
	
	private byte[] read(String key) {
		String filepath = keyFileMap.get(key);
		try {
			File f = new File(filepath);
			FileInputStream fis = new FileInputStream(f);
			BufferedInputStream bis = new BufferedInputStream(fis);
			
			byte[] value = new byte[(int) f.length()];	// TODO
			bis.read(value);
			
			bis.close();
			return value;
		}catch(NullPointerException e) {
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private boolean write(String key, byte[] value) {
		String filepath = selectDisk(key);
		try {
			File f = new File(filepath);
			FileOutputStream fos = new FileOutputStream(f);
			BufferedOutputStream bos = new BufferedOutputStream(fos);
			
			bos.write(value);	// TODO
			bos.flush();
			
			bos.close();
			return true;
		}catch(Exception e) {
			keyFileMap.remove(key);
			e.printStackTrace();
		}
		return false;
	}
	
	private boolean remove(String key) {
		String filepath = keyFileMap.get(key);
		keyFileMap.remove(key);
		try {
			File f = new File(filepath);
			return f.delete();
		}catch(NullPointerException e) {
			
		}catch(Exception e) {
			keyFileMap.put(key, filepath);
			e.printStackTrace();
		}
		return false;
	}
}
