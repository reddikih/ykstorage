package jp.ac.titech.cs.de.ykstorage.storage.buffer;

import jp.ac.titech.cs.de.ykstorage.storage.Block;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class LRUBuffer implements ReplacePolicy {

    private final static Logger logger = LoggerFactory.getLogger(LRUBuffer.class);

    private final Entry nil = new Entry(null, null, new Block(-1, -1, -1, -1, null));

    private final int CAPACITY;

    private int size;

    private HashMap<Block, Entry> hashMap = new HashMap<>();

    public LRUBuffer(int capacity) {
        this.CAPACITY = capacity;
        nil.next = nil;
        nil.prev = nil;
        logger.info("LRUBuffer created. Capacity:{}[entries]", this.CAPACITY);
    }

    private Entry insert(Block key) {
        Entry e = new Entry(null, null, key);
        e.next = nil.next;
        nil.next.prev = e;
        nil.next = e;
        e.prev = nil;

        hashMap.put(e.getKey(), e);
        size++;

        return null;
    }

    private Entry delete(Block key) {
        Entry e = hashMap.remove(key);
        e.prev.next = e.next;
        e.next.prev = e.prev;
        e.next = null;
        e.prev = null;
        size--;
        return e;
    }

    private Entry replace(Block key) {
        insert(key);
        return delete(nil.prev.getKey());
    }


    @Override
    public Block add(Block key) {
        synchronized (this) {
            Entry t = hashMap.get(key);
            if (t != null) {
                delete(t.getKey());
            }

            if (CAPACITY > size) {
                Entry result = insert(key);
                return result != null ? result.getKey() : null;
            } else {
                Entry result = replace(key);
                return result != null ? result.getKey() : null;
            }
        }
    }

    private class Entry {
        protected Entry prev;
        protected Entry next;
        private Block key;

        public Entry(Entry prev, Entry next, Block key) {
            this.prev = prev;
            this.next = next;
            this.key = key;
        }

        public Block getKey() {return this.key;}

        @Override
        public boolean equals(Object obj) {
            if(obj == this) return true;
            if (!(obj instanceof Entry)) return false;
            return ((Entry)obj).getKey() == this.getKey();
        }

        @Override
        public int hashCode() {
            return this.getKey().hashCode();
        }
    }
}
