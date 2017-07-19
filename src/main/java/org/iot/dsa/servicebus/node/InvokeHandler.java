package org.iot.dsa.servicebus.node;

import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.servicebus.node.MyDSActionNode.InboundInvokeRequestHandle;

public interface InvokeHandler {
	public ActionResult handle(DSMap parameters, InboundInvokeRequestHandle reqHandle);
}
