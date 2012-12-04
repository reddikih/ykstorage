package jp.ac.titech.cs.de.ykstorage.service;

public class Value {
	
	private byte[] value;
	
	// Null Object pattern
	public static final Value NULL = new Value(new byte[0]);
	
	public Value(byte[] value) {
		this.value = value;
	}
	
	public byte[] getValue() {
		return value;
	}

}
