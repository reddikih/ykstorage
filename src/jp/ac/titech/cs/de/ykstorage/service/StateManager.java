package jp.ac.titech.cs.de.ykstorage.service;

import java.io.IOException;
import java.util.logging.Logger;

import jp.ac.titech.cs.de.ykstorage.util.StorageLogger;


public class StateManager {
	public static final int ACTIVE = 0;
	public static final int IDLE = 1;
	public static final int STANDBY = 2;
	
	private int disknum;
	private int[] diskStates;	// TODO init
	private double[] idleIntimes;	// TODO init
	private double spindownThreshold;
	private int interval = 1000;
	private final Logger logger = StorageLogger.getLogger();
	
	private StateCheckThread sct;
	
	
	public StateManager(int disknum, double threshold) {
		this.disknum = disknum;
		this.diskStates = new int[disknum];
		this.idleIntimes = new double[disknum];
		this.spindownThreshold = threshold;
		this.sct = new StateCheckThread();
	}
	
	public void start() {
		sct.start();
	}
	
	public boolean spinup(int diskId) {
		if(diskId == 0) {
			return false;
		}
		String diskPath = Parameter.DATA_DISK_PATHS[diskId - 1];
		String[] cmdarray = {"ls", diskPath};
		int returnCode = this.execCommand(cmdarray);
		return (returnCode == 0) ? true : false;
	}
	
	public boolean spindown(int diskId) {
		if(diskId == 0) {
			return false;
		}
		char id = (char) (0x61 + diskId);
		String[] cmdarray = {"hdparm", "-y", "/dev/sd" + id};
		int returnCode = this.execCommand(cmdarray);
		return (returnCode == 0) ? true : false;
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
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return returnCode;
	}
	
	public boolean setDiskState(int diskId, int state) {
		diskStates[diskId] = state;
		return true;
	}
	
	public int getDiskState(int diskId) {
		return diskStates[diskId];
	}
	
	public boolean setIdleIntime(int diskId, long time) {
		idleIntimes[diskId] = time;
		return true;
	}
	
	public double getIdleIntime(int diskId) {
		return idleIntimes[diskId];
	}
	
	class StateCheckThread extends Thread {
		private boolean running = false;
		
		public void run() {
			running = true;
			while(running) {
				long now = System.currentTimeMillis();	// TODO long double
				for(int i = 0; i < disknum; i++) {
					if(getDiskState(i) == StateManager.IDLE && now - getIdleIntime(i) > spindownThreshold) {
						if(spindown(i)) {
							setDiskState(i, StateManager.STANDBY);
						}
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
	
}
