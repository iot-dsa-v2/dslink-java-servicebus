package org.iot.dsa.servicebus;

import org.iot.dsa.dslink.DSRequestException;
import org.iot.dsa.node.DSElement;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSMap.Entry;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.node.action.ActionSpec.ResultType;
import org.iot.dsa.security.DSPermission;
import org.iot.dsa.servicebus.node.MyDSActionNode;
import org.iot.dsa.servicebus.node.MyDSNode;
import org.iot.dsa.servicebus.node.MyValueType;
import org.iot.dsa.servicebus.node.MyDSActionNode.InboundInvokeRequestHandle;
import org.iot.dsa.servicebus.node.InvokeHandler;

import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.models.BrokeredMessage;
import com.microsoft.windowsazure.services.servicebus.models.QueueInfo;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMessageOptions;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveQueueMessageResult;

public class QueueNode extends MyDSNode implements ReceiverNode {
	
	private QueueInfo info;
	private ServiceBusNode serviceNode;
	
	/**
	 * Do not use
	 */
	public QueueNode() {
		super();
		this.info = new QueueInfo();
//		this.serviceNode = (ServiceBusNode) getParent().getParent();
	}

	public QueueNode(QueueInfo info, ServiceBusNode serviceNode) {
		super();
		this.info = info;
		this.serviceNode = serviceNode;
	}
	
	@Override
	public void onStart() {
		makeSendAction(true);
		makeReadAction(true);
		makeDeleteAction(true);
	}
	
	private void makeSendAction(boolean onStart) {
		MyDSActionNode act = new MyDSActionNode(DSPermission.READ, new InvokeHandler() {
			@Override
			public ActionResult handle(DSMap parameters, InboundInvokeRequestHandle reqHandle) {
				handleSend(parameters);
				return new ActionResult() {};
			}
		});
		act.addParameter("Message", null, MyValueType.STRING, null, null);
		act.addParameter("Properties", new DSMap(), null, null, null);
		addChild("Send_Message", act, onStart);
	}
	
	private void makeReadAction(boolean onStart) {
		MyDSActionNode act = new MyDSActionNode(DSPermission.READ, new ReceiveHandler(this));
		act.addParameter("Use_Peek-Lock", DSElement.make(true), null, null, null);
		act.setResultType(ResultType.STREAM_TABLE);
		addChild("Recieve_Messages", act, onStart);
	}
	
	private void makeDeleteAction(boolean onStart) {
		MyDSActionNode act = new MyDSActionNode(DSPermission.READ, new InvokeHandler() {
			@Override
			public ActionResult handle(DSMap parameters, InboundInvokeRequestHandle reqHandle) {
				handleDelete(parameters);
				return new ActionResult() {};
			}
    	});
		act.addParameter("Delete_From_Namespace", DSElement.make(false), null, null, null);
		addChild("Remove", act, onStart);
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
	
	@Override
	public void getMetaData(DSMap metaData) {
		super.getMetaData(metaData);
	}

}
