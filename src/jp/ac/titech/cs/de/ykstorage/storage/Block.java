package jp.ac.titech.cs.de.ykstorage.storage;

import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import net.jcip.annotations.GuardedBy;

import java.util.Arrays;

public class Block {

    public final static int BLOCK_SIZE = Parameter.BLOCK_SIZE;

    @GuardedBy("this") private long blockId;
    @GuardedBy("this") private int replicaLevel;
    @GuardedBy("this") private int primaryDiskId;
    @GuardedBy("this") private int diskGroupId;
    @GuardedBy("this") private int ownerDiskId;
    @GuardedBy("this") private byte[] payload;

    @Deprecated
    public Block(long blockId, int replicaLevel, int primaryDiskId, int diskGroupId, byte[] payload) {
        this(blockId, replicaLevel, primaryDiskId, diskGroupId, -1, payload);
    }

    public Block(long blockId, int replicaLevel, int primaryDiskId, int diskGroupId, int ownerDiskId, byte[] payload) {
        this.blockId = blockId;
        this.replicaLevel = replicaLevel;
        this.primaryDiskId = primaryDiskId;
        this.diskGroupId = diskGroupId;
        this.ownerDiskId = ownerDiskId;
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

    public synchronized int getOwnerDiskId() {return ownerDiskId;}

    public synchronized void setOwnerDiskId(int ownerDiskId) {this.ownerDiskId = ownerDiskId;}

    public synchronized byte[] getPayload() {return payload;}

    public synchronized void setPayload(byte[] payload) {this.payload = payload;}

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (!(obj instanceof Block) ) return false;

        synchronized (this) {
            Block bObj = (Block)obj;
            if (bObj.getBlockId() != this.getBlockId()) return false;
            if (bObj.getReplicaLevel() != this.getReplicaLevel()) return false;
            if (bObj.getPrimaryDiskId() != this.getPrimaryDiskId()) return false;
            if (bObj.getDiskGroupId() != this.getDiskGroupId()) return false;
            if (bObj.getOwnerDiskId() != this.getOwnerDiskId()) return false;
            if (!Arrays.equals(bObj.getPayload(), this.getPayload())) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        // refer to "Effective Java 2nd Edition", p.47
        int result = 17;

        synchronized (this) {
            result = 31 * result + (int)(getBlockId() ^ (getBlockId() >>> 32));
            result = 31 * result + getReplicaLevel();
            result = 31 * result + getPrimaryDiskId();
            result = 31 * result + getDiskGroupId();
            result = 31 * result + getOwnerDiskId();
            if(this.payload != null) {
                for (byte b : this.payload) {
                    result = 31 * result + (int)b;
                }
            }
        }
        return result;
    }

    @Override
    public String toString() {
        synchronized (this) {
            return String.format("BlockID:%d RepLevel:%d PrimaryDiskId:%d DiskGroupId:%d OwnerDiskId:%d PayloadLen:%d",
                    getBlockId(),
                    getReplicaLevel(),
                    getPrimaryDiskId(),
                    getDiskGroupId(),
                    getOwnerDiskId(),
                    getPayload() != null ? getPayload().length : 0);
        }
    }

}
