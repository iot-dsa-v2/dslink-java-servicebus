package org.iot.dsa.servicebus;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.iot.dsa.node.DSElement;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSString;
import org.iot.dsa.node.DSValueType;
import org.iot.dsa.node.action.ActionResultSpec;

public class Util {
	
	public static DSMap makeParameter(String name, DSElement def, MyValueType type, String description, String placeholder) {
		DSMap param = new DSMap();
		if (name == null || (def == null && type == null)) {
			return param;
		}
		param.put("name", name);
		if (def != null) {
			param.put("default", def);
		}
		if (type != null) {
			param.put("type", type.toString());
		}
		if (description != null) {
			param.put("description", description);
		}
		if (placeholder != null) {
			param.put("placeholder", placeholder);
		}
		return param;
	}
	
	public static class MyColumn implements ActionResultSpec {
		
		String name;
		DSValueType type;
		
		public MyColumn(String name, DSValueType type) {
			this.name = name;
			this.type = type;
		}

		@Override
		public DSMap getMetadata() {
			return null;
		}

		@Override
		public String getName() {
			return name;
		}

		@Override
		public DSValueType getType() {
			return type;
		}
	}
	
	public static class MyValueType {
		
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
			if (states != null) {
				sb.append("[").append(StringUtils.join(states, ',')).append("]");
			}
			return sb.toString();
		}
		
		public DSString encode() {
			return DSString.valueOf(toString());
		}

	}

}
