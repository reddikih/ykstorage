package test.jp.ac.titech.cs.de.ykstorage.frontend;

import jp.ac.titech.cs.de.ykstorage.frontend.ClientResponse;
import jp.ac.titech.cs.de.ykstorage.frontend.ResponseHeader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
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
        byte[] payload = generateContent(size, (byte)0x62);
        byte[] req = createWriteRequest(id, payload);
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

        try {
            // write
            Socket conn = new Socket(hostName, port);
            OutputStream out = conn.getOutputStream();

            byte[] payload = generateContent(size, (byte)0x62);
            byte[] request = createWriteRequest(id, payload);
            out.write(request);
            out.flush();

            ClientResponse response = new ClientResponse(conn);

            int responseStatus = response.getHeader().getStatus();
            assertThat(responseStatus, is(200));
            conn.close();

            // read
            conn = new Socket(hostName, port);
            out = conn.getOutputStream();

            request = createReadRequest(id);
            out.write(request);
            out.flush();

            response = new ClientResponse(conn);

            assertThat(response.getHeader().getStatus(), is(200));
            assertThat(response.getHeader().getKey(), is(id));

            byte[] responseContent = extractContent(response.getPayload(), 0, payload.length);
            assertThat(responseContent.length, is(payload.length));
            assertThat(responseContent, is(payload));

        } catch (IOException e) {
            e.printStackTrace();
            fail("IOException occurred.");
        }
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

    private byte[] createWriteRequest(long id, byte[] payload) {
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
                (byte)(0x000000ff & (payload.length >>> 24)),
                (byte)(0x000000ff & (payload.length >>> 16)),
                (byte)(0x000000ff & (payload.length >>> 8)),
                (byte)(0x000000ff & payload.length),
        };
        ByteBuffer buf = ByteBuffer.allocate(header.length + payload.length);
        return buf.put(header).put(payload).array();
    }

    private byte[] generateContent(int size, byte b) {
        byte[] result = new byte[size];
        for (int i=0; i < size; i++) {
            result[i] = b;
        }
        return result;
    }

    private byte[] extractContent(byte[] content, int start, int end) {
        return Arrays.copyOfRange(content, start, end);
    }
}
