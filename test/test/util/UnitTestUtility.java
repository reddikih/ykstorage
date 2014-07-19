package test.util;

import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.StorageManager;
import jp.ac.titech.cs.de.ykstorage.storage.StorageManagerFactory;

public class UnitTestUtility {

    public static Parameter getParameter(String configPath) {
        if (configPath == null) throw new NullPointerException();
        return new Parameter(configPath);
    }

    public static StorageManager createStorageManager(Parameter parameter) {
        StorageManagerFactory smFactory =
                StorageManagerFactory.getStorageManagerFactory(parameter);
        return smFactory.createStorageManager();
    }

    public static void setBufferConfiguration(long capacity, int blockSize, Parameter parameter) {
        parameter.bufferCapacity = capacity;
        parameter.BLOCK_SIZE = blockSize;
    }

    public static byte[] generateContent(int size, byte b) {
        byte[] result = new byte[size];
        for (int i=0; i < size; i++) {
            result[i] = b;
        }
        return result;
    }
}
