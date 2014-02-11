package jp.ac.titech.cs.de.ykstorage.storage.buffer;

public class DummyBuffer extends Buffer {

    public BufferFrame getFrame(Long key) {
        return null;
    }
}
