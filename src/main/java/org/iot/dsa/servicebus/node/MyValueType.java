package org.iot.dsa.servicebus.node;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.iot.dsa.node.DSValueType;

public class MyValueType {
	
	private DSValueType type;
	private List<String> states;
	
	private MyValueType(DSValueType type) {
		this(type, null);
	}
	
	private MyValueType(DSValueType type, List<String> states) {
		this.type = type;
		this.states = states;
	}

	public static final MyValueType ANY = new MyValueType(DSValueType.ANY);
	public static final MyValueType BINARY = new MyValueType(DSValueType.BINARY);
	public static final MyValueType BOOL = new MyValueType(DSValueType.BOOL);
	public static final MyValueType ENUM = new MyValueType(DSValueType.ENUM);
	public static final MyValueType LIST = new MyValueType(DSValueType.LIST);
	public static final MyValueType MAP = new MyValueType(DSValueType.MAP);
	public static final MyValueType NUMBER = new MyValueType(DSValueType.NUMBER);
	public static final MyValueType STRING = new MyValueType(DSValueType.STRING);
	
	public static MyValueType enumOf(List<String> states) {
		return new MyValueType(DSValueType.ENUM, states);
	}
	
	public static MyValueType boolOf(String trueString, String falseString) {
		return new MyValueType(DSValueType.BOOL, Arrays.asList(trueString, falseString));
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(type.toString());
		if (sb != null) {
			sb.append("[").append(StringUtils.join(states, ',')).append("]");
		}
		return sb.toString();
	}

}
