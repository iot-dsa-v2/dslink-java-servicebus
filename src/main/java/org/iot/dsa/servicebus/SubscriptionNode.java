package org.iot.dsa.servicebus;

import org.iot.dsa.dslink.DSRequestException;
import org.iot.dsa.node.DSElement;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.node.action.ActionSpec.ResultType;
import org.iot.dsa.security.DSPermission;
import org.iot.dsa.servicebus.node.InvokeHandler;
import org.iot.dsa.servicebus.node.MyDSActionNode;
import org.iot.dsa.servicebus.node.MyDSNode;
import org.iot.dsa.servicebus.node.MyDSActionNode.InboundInvokeRequestHandle;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.models.BrokeredMessage;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMessageOptions;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveSubscriptionMessageResult;
import com.microsoft.windowsazure.services.servicebus.models.SubscriptionInfo;

public class SubscriptionNode extends MyDSNode implements ReceiverNode {
	
	private SubscriptionInfo info;
	private TopicNode topicNode;
	
	/**
	 * Do not use
	 */
	public SubscriptionNode() {
		super();
		this.info = new SubscriptionInfo();
	}

	public SubscriptionNode(SubscriptionInfo info, TopicNode topicNode) {
		super();
		this.info = info;
		this.topicNode = topicNode;
	}
	
	@Override
	public void onStart() {
		makeReadAction(true);
		makeDeleteAction(true);
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
	
	
	private void handleDelete(DSMap parameters) {
		if (parameters.get("Delete_From_Namespace", false)) {
			try {
				topicNode.getService().deleteSubscription(topicNode.getTopicName(), info.getName());
			} catch (ServiceException e) {
				warn("Error Deleting Subscription: " + e);
				throw new DSRequestException(e.getMessage());
			}
		}
		delete();
	}

	@Override
	public BrokeredMessage receiveMessage(ReceiveMessageOptions opts) throws ServiceException {
		ReceiveSubscriptionMessageResult resultSubMsg;
		resultSubMsg = topicNode.getService().receiveSubscriptionMessage(topicNode.getTopicName(), info.getName(), opts);
		return resultSubMsg.getValue();
	}
	
	@Override
	public void deleteMessage(BrokeredMessage message) {
		try {
			topicNode.getService().deleteMessage(message);
		} catch (ServiceException e) {
			warn("Error Deleting Message: " + e);
		}
	}
	
}
