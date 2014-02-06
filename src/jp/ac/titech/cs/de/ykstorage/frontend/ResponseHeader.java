package jp.ac.titech.cs.de.ykstorage.frontend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

public class ResponseHeader {

    private static final Logger logger = LoggerFactory.getLogger(ResponseHeader.class);

    private int status;
    private long key;
    private int length;

    public ResponseHeader(Socket sock) throws IOException {
        parseHeader(sock);
    }

    private void parseHeader(Socket sock) throws IOException {
        InputStream in = sock.getInputStream();

        byte[] statusByte = new byte[2];
        int readByte = in.read(statusByte);
        if (readByte != statusByte.length)
            throw new IOException("couldn't read response status code.");
        for (byte b : statusByte)
            this.status = (this.status << 8) + (b & 0xff);

        byte[] keyByte = new byte[8];
        readByte = in.read(keyByte);
        if (readByte != keyByte.length)
            throw new IOException("couldn't read response key value.");
        for (byte b : keyByte)
            this.key = (this.key << 8) + (b & 0xff);

        byte[] lengthByte = new byte[4];
        readByte = in.read(lengthByte);
        if (readByte != lengthByte.length)
            throw new IOException("couldn't read response length.");
        for (byte b : lengthByte)
            this.length = (this.length << 8) + (b & 0xff);

        logger.debug("Response key:{}, status:{}, length:{}", getKey(), getStatus(), getLength());
    }

    public int getStatus() {return status;}
    public long getKey() {return key;}
    public int getLength() {return length;}
}
