package jp.ac.titech.cs.de.ykstorage.frontend;

public class ClientResponse {
    private ResponseHeader header;
    private byte[] payload;

    public ClientResponse(ResponseHeader header, byte[] payload) {
        this.header = header;
        this.payload = payload;
    }

    public ResponseHeader getHeader() {return header;}
    public byte[] getPayload() {return payload;}
}
