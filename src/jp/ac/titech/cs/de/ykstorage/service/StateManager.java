package jp.ac.titech.cs.de.ykstorage.service;

import java.io.IOException;
import java.util.logging.Logger;

import jp.ac.titech.cs.de.ykstorage.util.DiskState;
import jp.ac.titech.cs.de.ykstorage.util.StorageLogger;


public class StateManager {
//	public static final int ACTIVE = 0;
//	public static final int IDLE = 1;
//	public static final int STANDBY = 2;

	private int disknum;
	private DiskState[] diskStates;	// TODO init
	private double[] idleIntimes;	// TODO init
	private double spindownThreshold;
	private int interval = 1000;
	private final Logger logger = StorageLogger.getLogger();

	private StateCheckThread sct;


	public StateManager(int disknum, double threshold) {
		this.disknum = disknum;
		this.diskStates = initDiskStates(disknum);
		this.idleIntimes = new double[disknum];
		this.spindownThreshold = threshold;
		this.sct = new StateCheckThread();
	}

	private DiskState[] initDiskStates(int disknum) {
		DiskState[] result = new DiskState[disknum];
		for (int i = 0; i < disknum; i++) {
			result[i] = DiskState.ACTIVE;
		}
		return result;
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

	public boolean setDiskState(int diskId, DiskState state) {
		if(diskId == 0) {
			return false;
		}
		diskStates[diskId - 1] = state;
		return true;
	}

	public DiskState getDiskState(int diskId) {
		if(diskId == 0) {
			return DiskState.NA;
		}
		return diskStates[diskId - 1];
	}

	public boolean setIdleIntime(int diskId, long time) {
		if(diskId == 0) {
			return false;
		}
		idleIntimes[diskId - 1] = time;
		return true;
	}

	public double getIdleIntime(int diskId) {
		if(diskId == 0) {
			return -1.0;
		}
		return idleIntimes[diskId - 1];
	}

	class StateCheckThread extends Thread {
		private boolean running = false;

		public void run() {
			running = true;
			while(running) {
				long now = System.currentTimeMillis();	// TODO long double
				for(int i = 1; i < disknum + 1; i++) {
					if(getDiskState(i) == DiskState.IDLE && now - getIdleIntime(i) > spindownThreshold) {
						if(spindown(i)) {
							setDiskState(i, DiskState.STANDBY);
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
