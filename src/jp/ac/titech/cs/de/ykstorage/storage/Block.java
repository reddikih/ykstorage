package jp.ac.titech.cs.de.ykstorage.storage;

import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import net.jcip.annotations.GuardedBy;

public class Block {

    public final static int BLOCK_SIZE = Parameter.BLOCK_SIZE;

    @GuardedBy("this") private long blockId;
    @GuardedBy("this") private int replicaLevel;
    @GuardedBy("this") private int primaryDiskId;
    @GuardedBy("this") private int diskGroupId;
    @GuardedBy("this") private byte[] payload;

    public Block(long blockId, int replicaLevel, int primaryDiskId, int diskGroupId, byte[] payload) {
        this.blockId = blockId;
        this.replicaLevel = replicaLevel;
        this.primaryDiskId = primaryDiskId;
        this.diskGroupId = diskGroupId;
        this.payload = payload;
    }

    public synchronized long getBlockId() {return blockId;}

    public synchronized void setBlockId(long blockId) {this.blockId = blockId;}

    public synchronized int getReplicaLevel() {return replicaLevel;}

    public synchronized void setReplicaLevel(int replicaLevel) {this.replicaLevel = replicaLevel;}

    public synchronized int getPrimaryDiskId() {return this.primaryDiskId;}

    public synchronized void setPrimaryDiskId(int primaryDiskId) {this.primaryDiskId = primaryDiskId;}

    public synchronized int getDiskGroupId() {return diskGroupId;}

    public synchronized void setDiskGroupId(int diskGroupId) {this.diskGroupId = diskGroupId;}

    public synchronized byte[] getPayload() {return payload;}

    public synchronized void setPayload(byte[] payload) {this.payload = payload;}

}
