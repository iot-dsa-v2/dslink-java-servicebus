package org.iot.dsa.servicebus;

import org.iot.dsa.dslink.DSRequestException;
import org.iot.dsa.node.DSBool;
import org.iot.dsa.node.DSInfo;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSNode;
import org.iot.dsa.node.DSMap.Entry;
import org.iot.dsa.node.DSString;
import org.iot.dsa.node.action.ActionInvocation;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.node.action.DSAction;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.models.BrokeredMessage;
import com.microsoft.windowsazure.services.servicebus.models.QueueInfo;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMessageOptions;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveQueueMessageResult;

public class QueueNode extends ReceiverNode {
	
	private QueueInfo info;
	private ServiceBusNode serviceNode;
	
	/**
	 * Do not use
	 */
	public QueueNode() {
	}

	public QueueNode(QueueInfo info, ServiceBusNode serviceNode) {
		super();
		this.info = info;
		this.serviceNode = serviceNode;
	}
	
	@Override
	protected void declareDefaults() {
		super.declareDefaults();
		declareDefault("Send_Message", makeSendAction());
	}
	
	@Override
	protected void onStable() {
		if (serviceNode == null) {
			DSNode n = getParent();
			n = n.getParent();
			if (n instanceof ServiceBusNode) {
				serviceNode = ((ServiceBusNode) n);
			}
		}
		if (info == null) {
			info = new QueueInfo(getName());
		}
	}
	
	private DSAction makeSendAction() {
		DSAction act = new DSAction() {
			@Override
			public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
				((QueueNode) info.getParent()).handleSend(invocation.getParameters());
				return null;
			}
		};
		act.addParameter("Message", DSString.NULL, null);
		act.addDefaultParameter("Properties", new DSMap(), null);
		return act;
	}
	
	@Override
	protected DSAction makeRemoveAction() {
		DSAction act = new DSAction() {
			@Override
			public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
				((QueueNode) info.getParent()).handleDelete(invocation.getParameters());
				return null;
			}
    	};
		act.addDefaultParameter("Delete_From_Namespace", DSBool.FALSE, null);
		return act;
	}
	
	
	private void handleSend(DSMap parameters) {
		String messageText = parameters.getString("Message");
		DSMap properties = parameters.getMap("Properties");
		BrokeredMessage message = new BrokeredMessage(messageText);
		for (int i = 0; i < properties.size(); i++) {
			Entry entry = properties.getEntry(i);
			message.setProperty(entry.getKey(), entry.getValue().toString());
		}
		try {
			serviceNode.getService().sendQueueMessage(info.getPath(), message);
		} catch (ServiceException e) {
			warn("Error Sending Message: " + e);
			throw new DSRequestException(e.getMessage());
		}
	}
	
	private void handleDelete(DSMap parameters) {
		if (parameters.get("Delete_From_Namespace", false)) {
			try {
				serviceNode.getService().deleteQueue(info.getPath());;
			} catch (ServiceException e) {
				warn("Error Deleting Queue: " + e);
				throw new DSRequestException(e.getMessage());
			}
		}
		delete();
	}
	
	@Override
	public BrokeredMessage receiveMessage(ReceiveMessageOptions opts) throws ServiceException {
		ReceiveQueueMessageResult resultQM;
		resultQM = serviceNode.getService().receiveQueueMessage(info.getPath(), opts);
		return resultQM.getValue();
	}
	
	@Override
	public void deleteMessage(BrokeredMessage message) {
		try {
			serviceNode.getService().deleteMessage(message);
		} catch (ServiceException e) {
			warn("Error Deleting Message: " + e);
		}
	}
}
