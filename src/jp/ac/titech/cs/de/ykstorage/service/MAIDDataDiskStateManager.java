package jp.ac.titech.cs.de.ykstorage.service;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

import org.streamspinner.connection.CQException;
import org.streamspinner.connection.CQRowSet;
import org.streamspinner.connection.CQRowSetEvent;
import org.streamspinner.connection.CQRowSetListener;
import org.streamspinner.connection.DefaultCQRowSet;

import jp.ac.titech.cs.de.ykstorage.service.MAIDCacheDiskStateManager.GetDataThread;
import jp.ac.titech.cs.de.ykstorage.service.MAIDCacheDiskStateManager.GetDataThread.MyListener;
import jp.ac.titech.cs.de.ykstorage.service.MAIDCacheDiskStateManager.GetDataThread.MyListener2;
import jp.ac.titech.cs.de.ykstorage.util.DiskState;
import jp.ac.titech.cs.de.ykstorage.util.StorageLogger;


public class MAIDDataDiskStateManager {
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
	
	/**
	 * key: device file path
	 * value: disk state
	 */
	private Map<String, DiskState> diskStates;

	/**
	 * key: device file path
	 * value: start time of idle state
	 */
	private Map<String, Long> idleIntimes;
	
	/**
	 * key: device file path
	 * value: power of idle
	 */
	private Map<String, Double> wIdle;
	
	/**
	 * key: device file path
	 * value: power of standby
	 */
	private Map<String, Double> wStandby;
	
	/**
	 * key: device file path
	 * value: Joule of spinup
	 */
	private Map<String, Double> jSpinup;
	
	/**
	 * key: device file path
	 * value: Joule of spindown
	 */
	private Map<String, Double> jSpindown;

//	private double spindownThreshold;
	/**
	 * In this class, given spin down threshold time is converted
	 * from second(in double type) to millisecond(in long type) value
	 */
	private long spindownThreshold;
	
	/**
	 * key: device file path
	 * value: idle time threshold (spin down threshold)
	 */
	private Map<String, Long> tIdle;
	
	/**
	 * key: device file path
	 * value: standby time
	 */
	private Map<String, Long> tStandby;
	
	/**
	 * key: device file path
	 * value: isSpinup
	 */
	private Map<String, Boolean> isSpinup;
	
	/**
	 * key: device file path
	 * value: isSpindown
	 */
	private Map<String, Boolean> isSpindown;
	
	private double initWidle = 0.0;
	private double initWstandby = 0.0;
	private double initJspinup = 0.0;
	private double initJspindown = 0.0;
	private long initTstandby = 0L;

	private long interval;
	private String rmiUrl;
	private boolean[] isCacheDisk;
	private int numOfDevices;
	private int numOfCacheDisks;
	private int numOfDataDisks;
	private double acc;
	
	private String avgCommand;
	private String inteCommand;
	
	private final Logger logger = StorageLogger.getLogger();

	private StateCheckThread sct;
	private GetDataThread gdt;


	public MAIDDataDiskStateManager(Collection<String> devicePaths, double spinDownThreshold, long interval,
			String rmiUrl, boolean[] isCacheDisk, int numOfCacheDisks, int numOfDataDisks, double acc) {
		
		System.setProperty("java.security.policy","file:./security/StreamSpinner.policy");	// XXX
		
		this.diskStates = initDiskStates(devicePaths);
		this.idleIntimes = initIdleInTimes(devicePaths);
		this.spindownThreshold = (long)(spinDownThreshold * 1000);
		this.interval = interval;
		this.rmiUrl = rmiUrl;
		this.isCacheDisk = isCacheDisk;
		this.numOfDevices = this.diskStates.size();
		this.numOfCacheDisks = numOfCacheDisks;
		this.numOfDataDisks = numOfDataDisks;
		this.acc = acc;
		
		this.wIdle = initWJ(devicePaths, initWidle);
		this.wStandby = initWJ(devicePaths, initWstandby);
		this.jSpinup = initWJ(devicePaths, initJspinup);
		this.jSpindown = initWJ(devicePaths, initJspindown);
		this.tIdle = initT(devicePaths, spindownThreshold);
		this.tStandby = initT(devicePaths, initTstandby);
		
		makeSQLCommand();
		
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

	private Map<String, Long> initIdleInTimes(Collection<String> devicePaths) {
		Map<String, Long> result = new HashMap<String, Long>();
		long thisTime = System.currentTimeMillis();
		for (String device : devicePaths) {
			result.put(device, thisTime);
		}
		return result;
	}
	
	private Map<String, Double> initWJ(Collection<String> devicePaths, double data) {
		Map<String, Double> result = new HashMap<String, Double>();
		for (String device : devicePaths) {
			result.put(device, data);
		}
		return result;
	}
	
	private Map<String, Long> initT(Collection<String> devicePaths, long time) {
		Map<String, Long> result = new HashMap<String, Long>();
		for (String device : devicePaths) {
			result.put(device, time);
		}
		return result;
	}

	private boolean devicePathCheck(String devicePath) {
		boolean result = true;
		if(devicePath == null || devicePath == "") {
			result = false;
		}
		return result;
	}
	
	private void makeSQLCommand() {	// TODO avg and inte
		// e.g.) "MASTER Unit1 SELECT avg(Unit1.Power3),avg(Unit1.Power4) FROM Unit1[1000]"
		
		avgCommand = "MASTER Unit1 SELECT ";
		inteCommand = "MASTER Unit1 SELECT ";
		
		int numOfDisks = numOfCacheDisks + numOfDataDisks;
		for(int i = 0; i < numOfDisks; i++) {
			if(isCacheDisk[i]) {
				inteCommand = inteCommand.concat("inte(" + UNIT_NAMES[i] + "),");
			} else {
				avgCommand = avgCommand.concat("avg(" + UNIT_NAMES[i] + "),");
			}
		}
		
		avgCommand = avgCommand.substring(0, avgCommand.length() - 1) + " FROM Unit1[1000]";
		inteCommand = inteCommand.substring(0, inteCommand.length() - 1) + " FROM Unit1[1000]";
		
		logger.fine("MAID CacheDisk State [DataDisk AVG SQL Command]: " + avgCommand);
		logger.fine("MAID CacheDisk State [CacheDisk AVG SQL Command]: " + inteCommand);
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
			setIsSpinup(devicePath, true);
			initJspinup();
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
			return false;
		}
		
		String[] hdparm = {"hdparm", "-y", devicePath};
		int hdparmRet = execCommand(hdparm);
		if(hdparmRet == 0) {
			logger.fine("[SPINDOWN]: " + devicePath);
			setIsSpindown(devicePath, true);
			initJspindown();
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

	public boolean setIdleIntime(String devicePath, long time) {
		boolean result = true;
		if(!devicePathCheck(devicePath)) {
			result = false;
		}
		idleIntimes.put(devicePath, time);
		return result;
	}

	public double getIdleIntime(String devicePath) {
		if(!devicePathCheck(devicePath)) return -1.0;
		return idleIntimes.get(devicePath);
	}
	
	private synchronized boolean setWidle(String devicePath, double data) {
		boolean result = true;
		if(!devicePathCheck(devicePath)) {
			result = false;
		}
		wIdle.put(devicePath, data);
		return result;
	}
	
	private synchronized double getWidle(String devicePath) {
		if(!devicePathCheck(devicePath)) return -1.0;
		return wIdle.get(devicePath);
	}
	
	private synchronized boolean setWstandby(String devicePath, double data) {
		boolean result = true;
		if(!devicePathCheck(devicePath)) {
			result = false;
		}
		wStandby.put(devicePath, data);
		return result;
	}
	
	private synchronized double getWstandby(String devicePath) {
		if(!devicePathCheck(devicePath)) return -1.0;
		return wStandby.get(devicePath);
	}
	
	private synchronized boolean addJspinup(String devicePath, double data) {
		boolean result = true;
		if(!devicePathCheck(devicePath)) {
			result = false;
		}
		jSpinup.put(devicePath, jSpinup.get(devicePath) + data);
		return result;
	}
	
	private synchronized double getJspinup(String devicePath) {
		if(!devicePathCheck(devicePath)) return -1.0;
		return jSpinup.get(devicePath);
	}
	
	private synchronized void initJspinup() {
		Iterator<String> itr = jSpinup.keySet().iterator();
		while(itr.hasNext()) {
			String key = itr.next();
			jSpinup.put(key, 0.0);
		}
	}
	
	private synchronized boolean addJspindown(String devicePath, double data) {
		boolean result = true;
		if(!devicePathCheck(devicePath)) {
			result = false;
		}
		jSpindown.put(devicePath, jSpindown.get(devicePath) + data);
		return result;
	}
	
	private synchronized double getJspindown(String devicePath) {
		if(!devicePathCheck(devicePath)) return -1.0;
		return jSpindown.get(devicePath);
	}
	
	private synchronized void initJspindown() {
		Iterator<String> itr = jSpindown.keySet().iterator();
		while(itr.hasNext()) {
			String key = itr.next();
			jSpindown.put(key, 0.0);
		}
	}
	
	private synchronized boolean setTidle(String devicePath, long data) {
		boolean result = true;
		if(!devicePathCheck(devicePath)) {
			result = false;
		}
		tIdle.put(devicePath, data);
		return result;
	}
	
	private synchronized long getTidle(String devicePath) {
		if(!devicePathCheck(devicePath)) return -1L;
		return tIdle.get(devicePath);
	}
	
	private synchronized boolean setTstandby(String devicePath, long data) {
		boolean result = true;
		if(!devicePathCheck(devicePath)) {
			result = false;
		}
		tStandby.put(devicePath, data);
		return result;
	}
	
	private synchronized long getTstandby(String devicePath) {
		if(!devicePathCheck(devicePath)) return -1L;
		return tStandby.get(devicePath);
	}
	
	private synchronized boolean setIsSpinup(String devicePath, boolean data) {
		boolean result = true;
		if(!devicePathCheck(devicePath)) {
			result = false;
		}
		isSpinup.put(devicePath, data);
		return result;
	}
	
	private synchronized boolean getIsSpinup(String devicePath) {
		if(!devicePathCheck(devicePath)) return false;	// TODO false?
		return isSpinup.get(devicePath);
	}
	
	private synchronized boolean setIsSpindown(String devicePath, boolean data) {
		boolean result = true;
		if(!devicePathCheck(devicePath)) {
			result = false;
		}
		isSpindown.put(devicePath, data);
		return result;
	}
	
	private synchronized boolean getIsSpindown(String devicePath) {
		if(!devicePathCheck(devicePath)) return false;	// TODO false?
		return isSpindown.get(devicePath);
	}

	class StateCheckThread extends Thread {
		public void run() {
			while(true) {
				long now = System.currentTimeMillis();	// TODO long double

				for (String devicePath : diskStates.keySet()) {
					if (DiskState.IDLE.equals(getDiskState(devicePath)) &&
						(now - getIdleIntime(devicePath)) > getTidle(devicePath)) {
						spindown(devicePath);
					}
				}

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
			logger.fine("DataDisk GetDataThread [START]");
			try {
				CQRowSet rs = new DefaultCQRowSet();
				rs.setUrl(rmiUrl);   // StreamSpinnerの稼働するマシン名を指定
				rs.setCommand(avgCommand);   // 問合せのセット
				CQRowSetListener ls = new MyListener();
				rs.addCQRowSetListener(ls);   // リスナの登録
				rs.start();   // 問合せ処理の開始
			} catch(CQException e) {
				e.printStackTrace();
			}
			
			try {
				CQRowSet rs2 = new DefaultCQRowSet();
				rs2.setUrl(rmiUrl);   // StreamSpinnerの稼働するマシン名を指定
				rs2.setCommand(inteCommand);   // 問合せのセット
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
	    			while( rs.next() ){   // JDBCライクなカーソル処理により，１行ずつ処理結果を取得
	    				int i = 0;
	    				for (String devicePath : diskStates.keySet()) {
	    					if(DiskState.IDLE.equals(getDiskState(devicePath))) {
	    						setWidle(devicePath, rs.getDouble(i + 1));
	    					}else if(DiskState.STANDBY.equals(getDiskState(devicePath))) {
	    						setWstandby(devicePath, rs.getDouble(i + 1));
	    					}
	    					i++;
	    				}
	    			}
	    		} catch (CQException e1) {
	    			e1.printStackTrace();
				}
			}
		}
		
		class MyListener2 implements CQRowSetListener {
			public void dataDistributed(CQRowSetEvent e){   // 処理結果の生成通知を受け取るメソッド
				CQRowSet rs = (CQRowSet)(e.getSource());
	    		try {
	    			double wcurrent = 0.0;
	    			while( rs.next() ){   // JDBCライクなカーソル処理により，１行ずつ処理結果を取得
	    				int i = 0;
	    				wcurrent = (rs.getDouble(i + 1) * 100) / 100;	// TODO interval 100
	    				for (String devicePath : diskStates.keySet()) {
	    					if(getIsSpinup(devicePath)) {
	    						addJspinup(devicePath, rs.getDouble(i + 1));
	    					}else if(getIsSpindown(devicePath)) {
	    						addJspindown(devicePath, rs.getDouble(i + 1));
	    					}
	    					if(wcurrent < getWidle(devicePath) + acc) {	// TODO <,>,=
	    						setIsSpinup(devicePath, false);
	    					}
	    					if(wcurrent < getWstandby(devicePath) + acc) {
	    						setIsSpindown(devicePath, false);
	    					}
	    					i++;
	    				}
	    			}
	    			System.out.println(wcurrent);
	    		} catch (CQException e1) {
	    			e1.printStackTrace();
				}
			}
		}
	}

}
