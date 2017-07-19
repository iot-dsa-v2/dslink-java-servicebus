package org.iot.dsa.servicebus.node;

import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSValueType;
import org.iot.dsa.node.action.ActionResultSpec;

public class MyColumn implements ActionResultSpec {
	
	String name;
	DSValueType type;
	
	public MyColumn(String name, DSValueType type) {
		this.name = name;
		this.type = type;
	}

	@Override
	public DSMap getMetaData() {
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
