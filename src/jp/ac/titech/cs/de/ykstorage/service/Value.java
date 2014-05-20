package jp.ac.titech.cs.de.ykstorage.service;

import java.util.Arrays;

public class Value {
	
	private byte[] value;
	
	// Null Object pattern
	public static final Value NULL = new Value(new byte[0]);
	
	public Value(byte[] value) {
		if (value == null)
			throw new IllegalArgumentException();
		
		this.value = value;
	}
	
	public byte[] getValue() {
		return value;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Value)) {
			return false;
		}
		Value target = (Value)obj;		
		return Arrays.equals(this.getValue(), target.getValue());
	}

}
