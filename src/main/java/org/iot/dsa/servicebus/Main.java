package org.iot.dsa.servicebus;

import org.iot.dsa.dslink.DSLink;
import org.iot.dsa.dslink.DSLinkConfig;
import org.iot.dsa.dslink.DSRootNode;
import org.iot.dsa.node.DSInfo;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSString;
import org.iot.dsa.node.action.ActionInvocation;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.node.action.DSAction;

public class Main extends DSRootNode {
	
	private DSInfo addServiceBus = getInfo("Add_Service_Bus");
	
	@Override
    protected void declareDefaults() {
		DSAction act = new DSAction();
    	act.addParameter("Name", DSString.valueOf("danielbus"), null);
    	act.addParameter("Namespace", DSString.valueOf("danielbus"), null);
    	act.addParameter("SAS_Key_Name", DSString.valueOf("RootManageSharedAccessKey"), null);
    	act.addParameter("SAS_Key", DSString.valueOf("P+jvN1egFsUXuadbdPENAeIF5p2MglAbFDZLUVp8EGw="), null);
    	act.addParameter("Service_Bus_Root_Uri", DSString.valueOf(".servicebus.windows.net"), null);
    	declareDefault("Add_Service_Bus", act);
	}
	
	@Override
	public ActionResult onInvoke(DSInfo actionInfo, ActionInvocation invocation) {
        if (actionInfo == this.addServiceBus) {
            handleAddServiceBus(invocation.getParameters());
        }
        return null;
    }
    
    private void handleAddServiceBus(DSMap parameters) {
    	String name = parameters.getString("Name");
    	String namespace = parameters.getString("Namespace");
    	String keyName = parameters.getString("SAS_Key_Name");
    	String key = parameters.getString("SAS_Key");
    	String rootUri = parameters.getString("Service_Bus_Root_Uri");
    	ServiceBusNode sb = new ServiceBusNode(namespace, keyName, key, rootUri);
    	add(name, sb);
    }
    
    public static void main(String[] args) throws Exception {
		DSLinkConfig cfg = new DSLinkConfig(args);
        DSLink link = new DSLink(cfg);
        link.run();
	}

}
