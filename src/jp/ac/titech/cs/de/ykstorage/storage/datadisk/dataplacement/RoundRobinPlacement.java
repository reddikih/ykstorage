package jp.ac.titech.cs.de.ykstorage.storage.datadisk.dataplacement;


import net.jcip.annotations.GuardedBy;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinPlacement implements PlacementPolicy, Serializable {

    private final int numberOfDisks;

    @GuardedBy("this")
    private final AtomicInteger counter = new AtomicInteger(0);

    private final ConcurrentHashMap<Long, Integer> bid2did = new ConcurrentHashMap<>();

    public RoundRobinPlacement(int numberOfDisks) {
        this.numberOfDisks = numberOfDisks;
    }

    @Override
    public int assignDiskId(long blockId) {
        Integer assignedDiskId = bid2did.get(blockId);
        if (assignedDiskId == null) {
            synchronized (this) {
                assignedDiskId = counter.getAndIncrement() % numberOfDisks;
                bid2did.put(blockId, assignedDiskId);
            }
        }
        return assignedDiskId;
    }

}
