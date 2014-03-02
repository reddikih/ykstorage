package jp.ac.titech.cs.de.ykstorage.storage.buffer;

public interface ReplacePolicy<K> {

    public K add(K key);
}
