package test.jp.ac.titech.cs.de.ykstorage.frontend;

import jp.ac.titech.cs.de.ykstorage.frontend.FrontEnd;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith(JUnit4.class)
public class FrontEndTest {

    private int thread = 1;
    private int port = 9999;
    private String hostName = "localhost";

//    @Test(timeout=10000)
    @Test
    public void writeToFrontEnd() {
        long id = 223L;
        int size = 8;
        byte[] req = createWriteRequest(id, size);
        try {
            Socket conn = new Socket(this.hostName, this.port);
            InputStream in = conn.getInputStream();
            OutputStream out = conn.getOutputStream();

            out.write(req);
            out.flush();

            int response = parseResponseStatusCode(in);
            assertThat(response, is(200));

        } catch (IOException e) {
            e.printStackTrace();
            fail("IOException occurred.");
        }
    }

//    @Test
//    public void readFromFrontEnd() {
//        try {
//            FrontEnd frontend = FrontEnd.getInstance(thread, port);
//
//            Socket conn = new Socket(hostName, port);
//            InputStream in = conn.getInputStream();
//            OutputStream out = conn.getOutputStream();
//
//
//
//        } catch (IOException e) {
//            e.printStackTrace();
//            fail("IOException occurred.");
//        }
//    }

    private int parseResponseStatusCode(InputStream in) throws IOException {
        int result = 0;
        byte[] bytes = new byte[2];
        int ret = in.read(bytes);
        for (Byte b : bytes) {
            result = (result << 8) + (b & 0xff);
        }
        return result;
    }

    private byte[] createReadRequest(long id) {
        byte[] header = {
                0x00, 0x01,
                (byte)(0x00000000000000ff & (id >>> 56)) ,
                (byte)(0x00000000000000ff & (id >>> 48)) ,
                (byte)(0x00000000000000ff & (id >>> 40)) ,
                (byte)(0x00000000000000ff & (id >>> 32)) ,
                (byte)(0x00000000000000ff & (id >>> 24)) ,
                (byte)(0x00000000000000ff & (id >>> 16)) ,
                (byte)(0x00000000000000ff & (id >>> 8)) ,
                (byte)(0x00000000000000ff & (id))
        };
        return header;
    }

    private byte[] createWriteRequest(long id, int size) {
        byte[] header = {
                0x00, 0x10,
                (byte)(0x00000000000000ff & (id >>> 56)) ,
                (byte)(0x00000000000000ff & (id >>> 48)) ,
                (byte)(0x00000000000000ff & (id >>> 40)) ,
                (byte)(0x00000000000000ff & (id >>> 32)) ,
                (byte)(0x00000000000000ff & (id >>> 24)) ,
                (byte)(0x00000000000000ff & (id >>> 16)) ,
                (byte)(0x00000000000000ff & (id >>> 8)) ,
                (byte)(0x00000000000000ff & id),
                (byte)(0x000000ff & (size >>> 24)),
                (byte)(0x000000ff & (size >>> 16)),
                (byte)(0x000000ff & (size >>> 8)),
                (byte)(0x000000ff & size),
        };
        byte[] request = Arrays.copyOf(header, header.length + size);
        for (int i = header.length; i < request.length; i++) {
            request[i] = 0x62;
        }
        return request;
    }
}
