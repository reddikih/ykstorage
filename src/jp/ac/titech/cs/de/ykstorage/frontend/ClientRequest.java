package jp.ac.titech.cs.de.ykstorage.frontend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

public class ClientRequest {

    private final static Logger logger = LoggerFactory.getLogger(ClientRequest.class);

    private final RequestHeader header;
    private byte[] payload;

    public ClientRequest(Socket connection) throws IOException {
        this.header = new RequestHeader(connection);

        if (RequestCommand.EXIT.equals(this.header.getCommand())) {
            return;
        }

        this.payload = extractPayload(connection);
    }

    private byte[] extractPayload(Socket connection) throws IOException {
        byte[] result = null;
        if (this.header.getCommand() == RequestCommand.WRITE) {
            InputStream in = connection.getInputStream();
            result = new byte[this.header.getLength()];
            int readBytes = in.read(result);
            if (readBytes != result.length) {
                if (readBytes == 0) {
                    throw new IOException("couldn't read any bytes.");
                }
                logger.debug("request payload is incorrect. expected:{}[b] received:{}[b]",
                        result.length, readBytes);
            }

        }
        return result;
    }

    public RequestCommand getCommand() { return this.header.getCommand(); }
    public long getKey() { return this.header.getKey(); }
    public int getLength() { return this.header.getLength(); }
    public byte[] getPayload() { return this.payload; }
}
