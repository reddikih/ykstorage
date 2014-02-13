package jp.ac.titech.cs.de.ykstorage.frontend;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

public class ClientResponse {
    private ResponseHeader header;
    private byte[] payload;

    public ClientResponse(Socket connection) throws IOException {
        this.header = new ResponseHeader(connection);
        this.payload = extractPayload(connection);
    }

    private byte[] extractPayload(Socket connection) throws IOException {
        InputStream in = connection.getInputStream();
        byte[] result = new byte[this.header.getLength()];
        in.read(result);
        return result;
    }

    public ResponseHeader getHeader() {return header;}
    public byte[] getPayload() {return payload;}
}
