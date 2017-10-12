package org.iot.dsa.servicebus;

import org.iot.dsa.dslink.DSRootNode;
import org.iot.dsa.node.DSInfo;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSString;
import org.iot.dsa.node.DSValueType;
import org.iot.dsa.node.action.ActionInvocation;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.node.action.DSAction;


/**
 * This is the root node of the link.
 *
 * @author Daniel Shapiro
 */
public class RootNode extends DSRootNode {

    private DSInfo addServiceBus = getInfo("Add Service Bus");

    @Override
    protected void declareDefaults() {
        DSAction act = new DSAction();
        act.addParameter("Name", DSValueType.STRING, null);
        act.addParameter("Namespace", DSValueType.STRING, null);
        act.addDefaultParameter("SAS Key Name", DSString.valueOf("RootManageSharedAccessKey"),
                null);
        act.addParameter("SAS Key", DSValueType.STRING, null);
        act.addDefaultParameter("Service Bus Root Uri", DSString.valueOf(".servicebus.windows.net"),
                null);
        declareDefault("Add Service Bus", act);
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
        String keyName = parameters.getString("SAS Key Name");
        String key = parameters.getString("SAS Key");
        String rootUri = parameters.getString("Service Bus Root Uri");
        ServiceBusNode sb = new ServiceBusNode(namespace, keyName, key, rootUri);
        add(name, sb);
    }
}
