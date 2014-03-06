package jp.ac.titech.cs.de.ykstorage.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class ObjectSerializer<V> {

    private final static Logger logger = LoggerFactory.getLogger(ObjectSerializer.class);

    private final String filePathPrefix = System.getProperty("user.dir") + "/";
    private final String suffix = ".dat";

    public void serializeObject(V obj, String fileName) {
        ObjectOutputStream oos;

        try {
            oos = new ObjectOutputStream(new FileOutputStream(filePathPrefix + fileName + suffix));
            oos.writeObject(obj);

            logger.debug("Object {} is serialized.", filePathPrefix + fileName + suffix);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public V deSerializeObject(String fileName) {
        ObjectInputStream ois;
        V obj = null;

        try {
            File file = new File(filePathPrefix + fileName + suffix);
            if (!file.exists()) {
                logger.debug("Serialized file is not found: {}", filePathPrefix + fileName + suffix);
                return null;
            }

            ois = new ObjectInputStream(new FileInputStream(file));
            obj = (V)ois.readObject();

            logger.debug("Object {} is deserialized.", filePathPrefix + fileName + suffix);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return obj;
    }
}
