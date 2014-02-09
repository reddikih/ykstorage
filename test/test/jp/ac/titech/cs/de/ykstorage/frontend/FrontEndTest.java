package test.jp.ac.titech.cs.de.ykstorage.frontend;

import jp.ac.titech.cs.de.ykstorage.frontend.ResponseHeader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith(JUnit4.class)
public class FrontEndTest {

    private int thread;
    private int port;
    private String hostName;

    private final static Properties config = new Properties();
    private boolean isConfigured = false;

    @Before
    public void startUp() {
        if (!isConfigured) try {
            config.load(new FileInputStream("./test/test/jp/ac/titech/cs/de/ykstorage/frontend/server_info.properties"));

            this.thread = Integer.parseInt(config.getProperty("server.info.threads"));
            this.hostName = config.getProperty("server.info.hostname");
            this.port = Integer.parseInt(config.getProperty("server.info.port"));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


    @Test(timeout=15000)
    public void writeToFrontEnd() {
        long id = 223L;
        int size = 8;
        byte[] req = createWriteRequest(id, size);
        try {
            Socket conn = new Socket(this.hostName, this.port);
            OutputStream out = conn.getOutputStream();

            out.write(req);
            out.flush();

            ResponseHeader responseHeader = new ResponseHeader(conn);

            assertThat(responseHeader.getStatus(), is(200));
            assertThat(responseHeader.getKey(), is(id));
            assertThat(responseHeader.getLength(), is(size));

        } catch (IOException e) {
            e.printStackTrace();
            fail("IOException occurred.");
        }
    }

    @Test(timeout=15000)
    public void writeAndReadFromFrontEnd() {
        long id = 112233L;
        int size = 16;
        ResponseHeader responseHeader = null;

        try {
            // write
            Socket conn = new Socket(hostName, port);
            OutputStream out = conn.getOutputStream();

            byte[] request = createWriteRequest(id, size);
            out.write(request);
            out.flush();

            responseHeader = new ResponseHeader(conn);
            int responseStatus = responseHeader.getStatus();
            assertThat(responseStatus, is(200));
            conn.close();

            // read
            conn = new Socket(hostName, port);
            out = conn.getOutputStream();

            request = createReadRequest(id);
            out.write(request);
            out.flush();

            responseHeader = new ResponseHeader(conn);

            assertThat(responseHeader.getStatus(), is(200));
            assertThat(responseHeader.getKey(), is(id));
            assertThat(responseHeader.getLength(), is(size));
            assertThat(getReadPayload(conn, responseHeader.getLength()).length, is(size));

        } catch (IOException e) {
            e.printStackTrace();
            fail("IOException occurred.");
        }
    }

    private byte[] getReadPayload(Socket sock, int length) throws IOException {
        byte[] result = new byte[length];
        int ret = sock.getInputStream().read(result);
        return result;
    }

    private byte[] createReadRequest(long id) {
        byte[] request = {
                0x00, 0x01,
                (byte)(0x00000000000000ff & (id >>> 56)) ,
                (byte)(0x00000000000000ff & (id >>> 48)) ,
                (byte)(0x00000000000000ff & (id >>> 40)) ,
                (byte)(0x00000000000000ff & (id >>> 32)) ,
                (byte)(0x00000000000000ff & (id >>> 24)) ,
                (byte)(0x00000000000000ff & (id >>> 16)) ,
                (byte)(0x00000000000000ff & (id >>> 8)) ,
                (byte)(0x00000000000000ff & (id)),
                0x00,0x00,0x00,0x00,
        };
        return request;
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
