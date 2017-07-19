package org.iot.dsa.servicebus;

import org.iot.dsa.dslink.DSLink;
import org.iot.dsa.dslink.DSLinkConfig;
import org.iot.dsa.node.DSElement;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.security.DSPermission;
import org.iot.dsa.servicebus.node.MainDSNode;
import org.iot.dsa.servicebus.node.MyDSActionNode;
import org.iot.dsa.servicebus.node.MyDSActionNode.InboundInvokeRequestHandle;
import org.iot.dsa.servicebus.node.InvokeHandler;
import org.iot.dsa.servicebus.node.MyValueType;

public class Main extends MainDSNode {
    
    private void handleAddServiceBus(DSMap parameters) {
    	String name = parameters.getString("Name");
    	String namespace = parameters.getString("Namespace");
    	String keyName = parameters.getString("SAS_Key_Name");
    	String key = parameters.getString("SAS_Key");
    	String rootUri = parameters.getString("Service_Bus_Root_Uri");
    	ServiceBusNode sb = new ServiceBusNode(namespace, keyName, key, rootUri);
    	addChild(name, sb, false);
    }
	
    @Override
    public void onStart() {
    	MyDSActionNode act = new MyDSActionNode(DSPermission.READ, new InvokeHandler() {
			@Override
			public ActionResult handle(DSMap parameters, InboundInvokeRequestHandle reqHandle) {
				handleAddServiceBus(parameters);
				return new ActionResult() {};
			}
    	});
    	act.addParameter("Name", DSElement.make("danielbus"), MyValueType.STRING, null, null);
    	act.addParameter("Namespace", DSElement.make("danielbus"), MyValueType.STRING, null, null);
    	act.addParameter("SAS_Key_Name", DSElement.make("RootManageSharedAccessKey"), null, null, null);
    	act.addParameter("SAS_Key", DSElement.make("P+jvN1egFsUXuadbdPENAeIF5p2MglAbFDZLUVp8EGw="), MyValueType.STRING, null, null);
    	act.addParameter("Service_Bus_Root_Uri", DSElement.make(".servicebus.windows.net"), null, null, null);
    	addChild("Add_Service_Bus", act, true);
    }
    
    public static void main(String[] args) throws Exception {
		DSLinkConfig cfg = new DSLinkConfig(args)
                .setRootName("Service Bus")
                .setRootType(Main.class)
                .setBrokerUri("http://localhost:8080/conn");
        DSLink link = new DSLink(cfg);
        link.start();
	}

}
