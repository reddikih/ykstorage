package jp.ac.titech.cs.de.ykstorage.frontend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

public class ClientResponse {

    private final static Logger logger = LoggerFactory.getLogger(ClientResponse.class);

    private ResponseHeader header;
    private byte[] payload;

    public ClientResponse(Socket connection) throws IOException {
        this.header = new ResponseHeader(connection);
        this.payload = extractPayload(connection);
    }

    private byte[] extractPayload(Socket connection) throws IOException {
        InputStream in = connection.getInputStream();
        byte[] result = new byte[this.header.getLength()];
        int readBytes = in.read(result);
        if (readBytes != result.length) {
            if (readBytes == 0) {
                throw new IOException("couldn't read any bytes.");
            }
            logger.error("Response's payload is incorrect. expected:{}[b] received:{}[b]",
                    result.length, readBytes);
        }
        return result;
    }

    public ResponseHeader getHeader() {return header;}
    public byte[] getPayload() {return payload;}
}
