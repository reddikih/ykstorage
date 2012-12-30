package jp.ac.titech.cs.de.ykstorage.service;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

import jp.ac.titech.cs.de.ykstorage.util.DiskState;
import jp.ac.titech.cs.de.ykstorage.util.StorageLogger;

import org.streamspinner.connection.*;


public class MAIDCacheDiskStateManager {
	private static String[] UNIT_NAMES;
	static {
		int maxUnits = 6;
		int maxDisksPerUnit = 7;
		UNIT_NAMES = new String[maxUnits * maxDisksPerUnit];
		String prefix = "Unit1.Power%d";
		for(int i = 0; i < UNIT_NAMES.length; i++) {
			UNIT_NAMES[i] = String.format(prefix, i + 1);
		}
	}
	
	private String rmiUrl;
	private boolean[] isCacheDisk;

	private double wcache;
	private double[] wdata;
	private int spindownIndex;
	
	/**
	 * key: device file path
	 * value: disk state
	 */
	private Map<String, DiskState> diskStates;
	
	/**
	 * key: device file path
	 * value: access count
	 */
	private Map<String, Integer> accessCount;
	
	private double accessThreshold;
	
	private int numOfDevices;
	private int numOfCacheDisks;
	private int numOfDataDisks;
	
	private long interval;
	private final Logger logger = StorageLogger.getLogger();

	private StateCheckThread sct;
	private GetDataThread gdt;


	public MAIDCacheDiskStateManager(Collection<String> devicePaths, double accessThreshold, long interval, String rmiUrl, boolean[] isCacheDisk) {
		System.setProperty("java.security.policy","file:./security/StreamSpinner.policy");	// XXX
		
		this.diskStates = initDiskStates(devicePaths);
		this.numOfDevices = this.diskStates.size();
		this.accessCount = initAccessCount(devicePaths);
		this.wcache = 0.0;
		this.wdata = new double[devicePaths.size() + 1];// TODO devicePaths.size() + 1???
		this.spindownIndex = 0;
		this.accessThreshold = accessThreshold;
		this.interval = interval;
		this.rmiUrl = rmiUrl;
		this.isCacheDisk = isCacheDisk;
		
		this.numOfCacheDisks = 0;
		for(int i = 0; i < isCacheDisk.length; i++) {
			if(isCacheDisk[i]) numOfCacheDisks++;
		}
		
		this.sct = new StateCheckThread();
		this.gdt = new GetDataThread();
	}

	private Map<String, DiskState> initDiskStates(Collection<String> devicePaths) {
		Map<String, DiskState> result = new HashMap<String, DiskState>();
		for (String device : devicePaths) {
			result.put(device, DiskState.IDLE);
		}
		return result;
	}

	private Map<String, Integer> initAccessCount(Collection<String> devicePaths) {
		Map<String, Integer> result = new HashMap<String, Integer>();
		for (String device : devicePaths) {
//			result.put(device, 0);
			result.put(device, (int) accessThreshold + 1);
		}
		return result;
	}
	
	private synchronized void initAccessCount() {
		Iterator<String> itr = accessCount.keySet().iterator();
		while(itr.hasNext()) {
			String key = itr.next();
			accessCount.put(key, 0);
		}
	}

	private boolean devicePathCheck(String devicePath) {
		boolean result = true;
		if(devicePath == null || devicePath == "") {
			result = false;
		}
		return result;
	}

	public void start() {
		sct.start();
		gdt.start();
	}

	public boolean spinup(String devicePath) {
		if(!devicePathCheck(devicePath)) return false;
		setDiskState(devicePath, DiskState.IDLE);

		String[] cmdarray = {"ls", devicePath};
		int returnCode = execCommand(cmdarray);
		if(returnCode == 0) {
			logger.fine("[SPINUP]: " + devicePath);
			decSpindownIndex();
			return true;
		}
		setDiskState(devicePath, DiskState.STANDBY);
		return false;
	}

	public boolean spindown(String devicePath) {
		if(!devicePathCheck(devicePath)) return false;
		setDiskState(devicePath, DiskState.STANDBY);
		
		String[] sync = {"sync"};
		int syncRet = execCommand(sync);
		if(syncRet != 0) {
			setDiskState(devicePath, DiskState.IDLE);
			incSpindownIndex();
			return false;
		}
		
		String[] hdparm = {"hdparm", "-y", devicePath};
		int hdparmRet = execCommand(hdparm);
		if(hdparmRet == 0) {
			logger.fine("[SPINDOWN]: " + devicePath);
			return true;
		}
		setDiskState(devicePath, DiskState.IDLE);
		return false;
	}

	private int execCommand(String[] cmd) {
		int returnCode = 1;
		try {
			Runtime r = Runtime.getRuntime();
			Process p = r.exec(cmd);
			returnCode = p.waitFor();
			if(returnCode != 0) {
				logger.info(cmd[0] + " return code: " + returnCode);
			}
		} catch (IOException e) {
//			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return returnCode;
	}

	public boolean setDiskState(String devicePath, DiskState state) {
		boolean result = true;
		if(!devicePathCheck(devicePath))
			result = false;
		this.diskStates.put(devicePath, state);
		return result;
	}

	public DiskState getDiskState(String devicePath) {
		if(!devicePathCheck(devicePath)) return DiskState.NA;

		DiskState state = diskStates.get(devicePath);
		if (state == null) {
			state = DiskState.NA;
		}
		return state;
	}

	public synchronized boolean incAccessCount(String devicePath) {
		boolean result = true;
		if(!devicePathCheck(devicePath)) {
			result = false;
		}
		accessCount.put(devicePath, accessCount.get(devicePath) + 1);
		return result;
	}

	private synchronized int getAccessCount(String devicePath) {
		if(!devicePathCheck(devicePath)) return -1;
		return accessCount.get(devicePath);
	}
	
	private synchronized void addWdata(double data) {
		wdata[spindownIndex] += data;
	}
	
	private synchronized double getWdata(int index) {
		return wdata[index];
	}
	
	private synchronized void incSpindownIndex() {
		if(spindownIndex < numOfDevices) {
			spindownIndex++;
		}
	}
	
	private synchronized void decSpindownIndex() {
		if(spindownIndex > 0) {
			spindownIndex--;
		}
	}
	
	private synchronized int getSpindownIndex() {
		return spindownIndex;
	}
	
	class StateCheckThread extends Thread {
		public void run() {
			while(true) {
				for (String devicePath : diskStates.keySet()) {
					if ((double)getAccessCount(devicePath) / (double)interval < accessThreshold) {
						logger.fine("[PROPOSAL1]: spindown" + devicePath);
						spindown(devicePath);
					}
				}
				
				int index = getSpindownIndex();
				if(index > 0) {
					if(getWdata(index) - getWdata(index-1) > wcache) {
						String tmpDevice = "";
						for (String devicePath : diskStates.keySet()) {
							if(DiskState.SPINDOWN.equals(getDiskState(devicePath))) {
								tmpDevice = devicePath;
								break;
							}
						}
						logger.fine("[PROPOSAL1]: spinup" + tmpDevice);
						spinup(tmpDevice);
					}
				}

				// init
				initAccessCount();
				
				try {
					sleep(interval);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	class GetDataThread extends Thread {
		public void run() {
			try {
				CQRowSet rs = new DefaultCQRowSet();
				rs.setUrl(rmiUrl);   // StreamSpinnerの稼働するマシン名を指定
				rs.setCommand("MASTER Unit1 SELECT avg(Unit1.Power3),avg(Unit1.Power4) FROM Unit1[1000]");   // 問合せのセット
				CQRowSetListener ls = new MyListener();
				rs.addCQRowSetListener(ls);   // リスナの登録
				rs.start();   // 問合せ処理の開始
				logger.fine("GetDataThread [START]");
			} catch(CQException e) {
				e.printStackTrace();
			}
			
			try {
				CQRowSet rs2 = new DefaultCQRowSet();
				rs2.setUrl(rmiUrl);   // StreamSpinnerの稼働するマシン名を指定
				rs2.setCommand("MASTER Unit1 SELECT avg(Unit1.Power1),avg(Unit1.Power2) FROM Unit1[1000]");   // 問合せのセット
				CQRowSetListener ls2 = new MyListener2();
				rs2.addCQRowSetListener(ls2);   // リスナの登録
				rs2.start();   // 問合せ処理の開始
			} catch(CQException e) {
				e.printStackTrace();
			}
		}
		
		class MyListener implements CQRowSetListener {
	    	public void dataDistributed(CQRowSetEvent e){   // 処理結果の生成通知を受け取るメソッド
	    		CQRowSet rs = (CQRowSet)(e.getSource());
	    		try {
	    			int index = getSpindownIndex();
	    			wdata[index] = 0.0;
	    			int i = 0;
	    			while( rs.next() ){   // JDBCライクなカーソル処理により，１行ずつ処理結果を取得
	    				addWdata(rs.getDouble(i + 1));
	    				i++;
	    			}
	    			System.out.println(index + ": " + getWdata(getSpindownIndex()));
	    		} catch (CQException e1) {
	    			e1.printStackTrace();
				}
	        }
	    }
		
		class MyListener2 implements CQRowSetListener {
	    	public void dataDistributed(CQRowSetEvent e){   // 処理結果の生成通知を受け取るメソッド
	    		CQRowSet rs = (CQRowSet)(e.getSource());
	    		try {
	    			wcache = 0.0;
	    			int i = 0;
	    			while( rs.next() ){   // JDBCライクなカーソル処理により，１行ずつ処理結果を取得
	    				wcache += rs.getDouble(i + 1);
	    				i++;
	    			}
	    			wcache = wcache / (double) numOfCacheDisks;
	    			System.out.println(wcache);
	    		} catch (CQException e1) {
	    			e1.printStackTrace();
				}
	        }
	    }
	}

}
