package jp.ac.titech.cs.de.ykstorage.storage;

import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import net.jcip.annotations.GuardedBy;

public class Block {

    public final static int BLOCK_SIZE = Parameter.BLOCK_SIZE;

    @GuardedBy("this") private long key;
    @GuardedBy("this") private int replicaLevel;
    @GuardedBy("this") private String primaryDisk;
    @GuardedBy("this") private int diskGroupId;

    public Block(long key, int replicaLevel, String primaryDisk, int diskGroupId) {
        this.key = key;
        this.replicaLevel = replicaLevel;
        this.primaryDisk = primaryDisk;
        this.diskGroupId = diskGroupId;
    }

    public synchronized long getKey() {return key;}

    public synchronized void setKey(long key) {this.key = key;}

    public synchronized int getReplicaLevel() {return replicaLevel;}

    public synchronized void setReplicaLevel(int replicaLevel) {this.replicaLevel = replicaLevel;}

    public synchronized String getPrimaryDisk() {return primaryDisk;}

    public synchronized void setPrimaryDisk(String primaryDisk) {this.primaryDisk = primaryDisk;}

    public synchronized int getDiskGroupId() {return diskGroupId;}

    public synchronized void setDiskGroupId(int diskGroupId) {this.diskGroupId = diskGroupId;}

}
