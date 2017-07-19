package org.iot.dsa.servicebus;

import java.util.ArrayList;
import java.util.List;

import org.iot.dsa.node.DSElement;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSMap.Entry;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.security.DSPermission;
import org.iot.dsa.servicebus.node.MyDSActionNode;
import org.iot.dsa.servicebus.node.MyDSNode;
import org.iot.dsa.servicebus.node.MyValueType;
import org.iot.dsa.servicebus.node.Utils;
import org.iot.dsa.servicebus.node.MyDSActionNode.InboundInvokeRequestHandle;
import org.iot.dsa.servicebus.node.InvokeHandler;

import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;
import com.microsoft.windowsazure.services.servicebus.models.BrokeredMessage;
import com.microsoft.windowsazure.services.servicebus.models.ListSubscriptionsResult;
import com.microsoft.windowsazure.services.servicebus.models.SubscriptionInfo;
import com.microsoft.windowsazure.services.servicebus.models.TopicInfo;

public class TopicNode extends MyDSNode {
	
	private TopicInfo info;
	private ServiceBusNode serviceNode;

	/**
	 * Do not use
	 */
	public TopicNode() {
		super();
		this.info = new TopicInfo();
//		this.serviceNode = (ServiceBusNode) getParent().getParent();
	}
	
	public TopicNode(TopicInfo info, ServiceBusNode serviceNode) {
		super();
		this.info = info;
		this.serviceNode = serviceNode;
	}
	
	public ServiceBusContract getService() {
		return serviceNode.getService();
	}
	
	public String getTopicName() {
		return info.getPath();
	}
	
	@Override
	public void onStart() {
		makeSendAction(true);
		makeCreateSubscriptionAction(true);
		makeDeleteAction(true);
		makeRefreshAction(true);
		
		init(true);
	}
	
	private void init(boolean onStart) {
		try {
			ListSubscriptionsResult result = getService().listSubscriptions(info.getPath());
			makeAddSubscriptionAction(result.getItems(), onStart);
		} catch (ServiceException e) {
			warn("Error listing subscriptions: " + e);
		}
	}
	
	private void makeRefreshAction(boolean onStart) {
		MyDSActionNode act = new MyDSActionNode(DSPermission.READ, new InvokeHandler() {
			@Override
			public ActionResult handle(DSMap parameters, InboundInvokeRequestHandle reqHandle) {
				init(false);
				return new ActionResult() {};
			}
    	});
		addChild("Refresh", act, onStart);
	}
	
	private void makeAddSubscriptionAction(List<SubscriptionInfo> subscriptions, boolean onStart) {
		List<String> subNames = new ArrayList<String>();
		for (SubscriptionInfo sInfo: subscriptions) {
			subNames.add(sInfo.getName());
		}
		MyDSActionNode act = new MyDSActionNode(DSPermission.READ, new InvokeHandler() {
			@Override
			public ActionResult handle(DSMap parameters, InboundInvokeRequestHandle reqHandle) {
				addSubscription(parameters);
				return new ActionResult() {};
			}
		});
		act.addParameter("Subscription_Name", null, MyValueType.enumOf(subNames), null, null);
		addChild("Add_Subscription", act, onStart);	
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
		act.addParameter("Properties", new DSMap(), MyValueType.MAP, null, null);
		addChild("Send_Message", act, onStart);
	}
	
	private void makeCreateSubscriptionAction(boolean onStart) {
		MyDSActionNode act = new MyDSActionNode(DSPermission.READ, new InvokeHandler() {
			@Override
			public ActionResult handle(DSMap parameters, InboundInvokeRequestHandle reqHandle) {
				createSubscription(parameters);
				return new ActionResult() {};
			}
    	});
		act.addParameter("Name", null, MyValueType.STRING, null, null);
		addChild("Create_Subscription", act, onStart);
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
	
	
	private void addSubscription(DSMap parameters) {
		String name = parameters.getString("Subscription_Name");
		SubscriptionInfo sInfo = new SubscriptionInfo(name);
		addChild(sInfo.getName(), new SubscriptionNode(sInfo, this), false);
	}
	
	private void handleSend(DSMap parameters) {
		String messageText = parameters.getString("Message");
		DSMap properties = Utils.getMap(parameters, "Properties");
		BrokeredMessage message = new BrokeredMessage(messageText);
		for (int i = 0; i < properties.size(); i++) {
			Entry entry = properties.getEntry(i);
			message.setProperty(entry.getKey(), entry.getValue().toString());
		}
		try {
			getService().sendTopicMessage(info.getPath(), message);
		} catch (ServiceException e) {
			// TODO Send Error
			warn("Error Sending Message: " + e);
		}
	}
	
	private void createSubscription(DSMap parameters) {
		String name = parameters.getString("Name");
		SubscriptionInfo sinfo = new SubscriptionInfo(name);
		try {
			getService().createSubscription(info.getPath(), sinfo);
			addChild(sinfo.getName(), new SubscriptionNode(sinfo, this), false);
		} catch (ServiceException e) {
			// TODO Send Error
			warn("Error Creating Subscription: " + e);
		}
	}
	
	private void handleDelete(DSMap parameters) {
		if (parameters.get("Delete_From_Namespace", false)) {
			try {
				getService().deleteTopic(info.getPath());
			} catch (ServiceException e) {
				// TODO Send Error
				warn("Error Deleting Topic: " + e);
			}
		}
		delete();
	}
	

}
