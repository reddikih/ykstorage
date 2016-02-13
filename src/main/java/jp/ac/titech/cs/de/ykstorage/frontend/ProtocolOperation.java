package jp.ac.titech.cs.de.ykstorage.frontend;

public class ProtocolOperation {

    public static byte[] int2bytes(int value) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte)((value >>> 24) & 0xff);
        bytes[1] = (byte)((value >>> 16) & 0xff);
        bytes[2] = (byte)((value >>>  8) & 0xff);
        bytes[3] = (byte)(value & 0xff);
        return bytes;
    }

    public static byte[] long2bytes(long value) {
        byte[] bytes = new byte[8];
        bytes[0] = (byte)((value >>> 56) & 0xff);
        bytes[1] = (byte)((value >>> 48) & 0xff);
        bytes[2] = (byte)((value >>> 40) & 0xff);
        bytes[3] = (byte)((value >>> 32) & 0xff);
        bytes[4] = (byte)((value >>> 24) & 0xff);
        bytes[5] = (byte)((value >>> 16) & 0xff);
        bytes[6] = (byte)((value >>>  8) & 0xff);
        bytes[7] = (byte)(value & 0xff);
        return bytes;
    }
}
