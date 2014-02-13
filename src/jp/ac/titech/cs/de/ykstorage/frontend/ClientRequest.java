package jp.ac.titech.cs.de.ykstorage.frontend;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

public class ClientRequest {
    private final RequestHeader header;
    private byte[] payload;

    public ClientRequest(Socket connection) throws IOException {
        this.header = new RequestHeader(connection);
        this.payload = extractPayload(connection);
    }

    private byte[] extractPayload(Socket connection) throws IOException {
        byte[] result = null;
        if (this.header.getCommand() == RequestCommand.WRITE) {
            InputStream in = connection.getInputStream();
            result = new byte[this.header.getLength()];
            int readBytes = in.read(result);
            if (readBytes != result.length)
                throw new IOException("request payload is incorrect.");
        }
        return result;
    }

    public RequestCommand getCommand() { return this.header.getCommand(); }
    public long getKey() { return this.header.getKey(); }
    public int getLength() { return this.header.getLength(); }
    public byte[] getPayload() { return this.payload; }
}
