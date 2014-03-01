package jp.ac.titech.cs.de.ykstorage.cli;

import java.nio.ByteBuffer;


public class Request {
	private final byte[] type;
	private final long delay;
    private final long id;
    private final int size;
    
    public Request(byte[] type, long delay, long id, int size) {
    	this.type = type;
    	this.delay = delay;
    	this.id = id;
    	this.size = size;
    }
    
    public Request(byte[] type, long delay, long id) {
    	this.type = type;
    	this.delay = delay;
    	this.id = id;
    	this.size = 0;
    }
    
    public byte[] getRequest() {
    	byte[] request;
    	
    	if(type[0] == 0x00 && type[1] == 0x01) {
    		request = createReadRequest(id);
    	} else if(type[0] == 0x00 && type[1] == 0x10) {
    		byte[] payload = generateContent(size, (byte)0x62);
    		request = createWriteRequest(id, payload);
    	} else {
    		request = new byte[1];
    	}
    	
    	return request;
    }
    
    public long getDelay() {
    	return delay;
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
    
}
