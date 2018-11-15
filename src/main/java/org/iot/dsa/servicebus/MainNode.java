package org.iot.dsa.servicebus;

import org.iot.dsa.dslink.DSMainNode;
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
public class MainNode extends DSMainNode {

    @Override
    protected void declareDefaults() {
        DSAction act = new DSAction.Parameterless() {
            @Override
            public ActionResult invoke(DSInfo target, ActionInvocation invocation) {
                ((MainNode) target.get()).handleAddServiceBus(invocation.getParameters());
                return null;
            }
        };
        act.addParameter("Name", DSValueType.STRING, null);
        act.addParameter("Namespace", DSValueType.STRING, null);
        act.addDefaultParameter("SAS Key Name", DSString.valueOf("RootManageSharedAccessKey"),
                                null);
        act.addParameter("SAS Key", DSValueType.STRING, null);
        act.addDefaultParameter("Service Bus Root Uri", DSString.valueOf(".servicebus.windows.net"),
                                null);
        declareDefault("Add Service Bus", act);
        declareDefault("Docs", DSString.valueOf(
                "https://github.com/iot-dsa-v2/dslink-java-v2-servicebus/blob/develop/README.md"))
                .setTransient(true).setReadOnly(true);
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
