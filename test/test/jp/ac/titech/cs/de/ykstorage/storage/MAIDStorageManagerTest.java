package test.jp.ac.titech.cs.de.ykstorage.storage;

import java.util.Arrays;
import jp.ac.titech.cs.de.ykstorage.service.Parameter;
import jp.ac.titech.cs.de.ykstorage.storage.MAIDStorageManager;
import jp.ac.titech.cs.de.ykstorage.storage.StorageManager;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import test.util.UnitTestUtility;

@RunWith(JUnit4.class)
public class MAIDStorageManagerTest {

    private final static String CONF_PATH = "./config/test/maid_sm_unittest.properties";

    @Test
    public void checkCorrectStorageManager() {
        Parameter parameter = UnitTestUtility.getParameter(CONF_PATH);
        UnitTestUtility.setBufferConfiguration(40, 8, parameter);
        StorageManager storageManager = UnitTestUtility.createStorageManager(parameter);
        assertThat((storageManager instanceof MAIDStorageManager), is(true));
    }

    @Test
    public void simpleWrite() {
        Parameter parameter = UnitTestUtility.getParameter(CONF_PATH);
        UnitTestUtility.setBufferConfiguration(40, 8, parameter);
        StorageManager storageManager = UnitTestUtility.createStorageManager(parameter);

        byte[] payload = UnitTestUtility.generateContent(8, (byte)'e');
        boolean result = storageManager.write(0L, payload);
        assertThat(result, is(true));
    }

    @Test
    public void oneWriteAndRead() {
        Parameter parameter = UnitTestUtility.getParameter(CONF_PATH);
        UnitTestUtility.setBufferConfiguration(40, 8, parameter);
        StorageManager storageManager = UnitTestUtility.createStorageManager(parameter);

        byte[] payload = UnitTestUtility.generateContent(8, (byte)'f');
        boolean result = storageManager.write(0L, payload);
        assertThat(result, is(true));

        byte[] read = storageManager.read(0L);
        assertThat(read, notNullValue());
        assertThat((Arrays.equals(payload, read)), is(true));
    }

    @Test
    public void writeWithReplaceBufferEntries() {
        Parameter parameter = UnitTestUtility.getParameter(CONF_PATH);
        UnitTestUtility.setBufferConfiguration(40, 8, parameter); // buffer entries: 5
        StorageManager storageManager = UnitTestUtility.createStorageManager(parameter);

        boolean result;
        byte[] b_a = UnitTestUtility.generateContent(8, (byte)'a');
        result = storageManager.write(0L, b_a);
        assertThat(result, is(true));
        byte[] b_b = UnitTestUtility.generateContent(8, (byte)'b');
        result = storageManager.write(1L, b_b);
        assertThat(result, is(true));
        byte[] b_c = UnitTestUtility.generateContent(8, (byte)'c');
        result = storageManager.write(2L, b_c);
        assertThat(result, is(true));
        byte[] b_d = UnitTestUtility.generateContent(8, (byte)'d');
        result = storageManager.write(3L, b_d);
        assertThat(result, is(true));
        byte[] b_e = UnitTestUtility.generateContent(8, (byte)'e');
        result = storageManager.write(4L, b_e);
        assertThat(result, is(true));

        byte[] read;
        read  = storageManager.read(0L);
        assertThat((Arrays.equals(b_a, read)), is(true));
        read  = storageManager.read(1L);
        assertThat((Arrays.equals(b_b, read)), is(true));
        read  = storageManager.read(2L);
        assertThat((Arrays.equals(b_c, read)), is(true));
        read  = storageManager.read(3L);
        assertThat((Arrays.equals(b_d, read)), is(true));
        read  = storageManager.read(4L);
        assertThat((Arrays.equals(b_e, read)), is(true));

        byte[] b_f = UnitTestUtility.generateContent(8, (byte)'f');
        result = storageManager.write(5L, b_f);
        assertThat(result, is(true));

        read  = storageManager.read(0L);
        assertThat((Arrays.equals(b_a, read)), is(true));
        // And you should confirm that this read from data
        // disk due to replaced by previous write

        read  = storageManager.read(5L);
        assertThat((Arrays.equals(b_f, read)), is(true));
    }
}
