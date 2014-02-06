package jp.ac.titech.cs.de.ykstorage.frontend;

/**
 * Created by hikida on 14/02/05.
 */
public class ClientRequest {
    private final RequestHeader header;
    private byte[] payload;

    protected ClientRequest(RequestHeader header, byte[] payload) {
        this.header = header;
        this.payload = payload;
    }

    public RequestCommand getCommand() { return this.header.getCommand(); }
    public long getKey() { return this.header.getKey(); }
    public int getLength() { return this.header.getLength(); }
    public byte[] getPayload() { return this.payload; }
}
