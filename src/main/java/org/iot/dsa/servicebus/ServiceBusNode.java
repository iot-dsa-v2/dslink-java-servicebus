package org.iot.dsa.servicebus;

import java.util.ArrayList;
import java.util.List;

import org.iot.dsa.node.DSElement;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.security.DSPermission;
import org.iot.dsa.servicebus.node.MyDSActionNode;
import org.iot.dsa.servicebus.node.MyDSNode;
import org.iot.dsa.servicebus.node.MyDSValueNode;
import org.iot.dsa.servicebus.node.MyValueType;
import org.iot.dsa.servicebus.node.MyDSActionNode.InboundInvokeRequestHandle;
import org.iot.dsa.servicebus.node.MyDSActionNode.InvokeHandler;

import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.ServiceBusConfiguration;
import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;
import com.microsoft.windowsazure.services.servicebus.ServiceBusService;
import com.microsoft.windowsazure.services.servicebus.models.ListQueuesResult;
import com.microsoft.windowsazure.services.servicebus.models.ListTopicsResult;
import com.microsoft.windowsazure.services.servicebus.models.QueueInfo;
import com.microsoft.windowsazure.services.servicebus.models.TopicInfo;


public class ServiceBusNode extends MyDSNode {
	
	private String namespace;
	private String keyName;
	private String key;
	private String rootUri;
	
	private ServiceBusContract service;
	
	private MyDSValueNode statNode;
	private MyDSNode queuesNode;
	private MyDSNode topicsNode;
	
	/**
	 * Do not use
	 */
	public ServiceBusNode() {
		super();
		this.namespace = "";
		this.keyName = "RootManageSharedAccessKey";
		this.key = "";
		this.rootUri = ".servicebus.windows.net";
	}
	
	public ServiceBusNode(String namespace, String keyName, String key, String rootUri) {
		super();
		this.namespace = namespace;
		this.keyName = keyName;
		this.key = key;
		this.rootUri = rootUri;
	}
	
	public ServiceBusContract getService() {
		return service;
	}
	
	@Override
	public void onStart() {
		statNode = new MyDSValueNode();
		statNode.setValue(DSElement.make("Connecting"));
		addChild("STATUS", statNode, true);
		
		queuesNode = new MyDSNode();
		addChild("Queues", queuesNode, true);
		
		topicsNode = new MyDSNode();
		addChild("Topics", topicsNode, true);
		
		makeRemoveAction(true);
		makeRefreshAction(true);
		makeCreateQueueAction(true);
		makeCreateTopicAction(true);
		
		init(true);
	}
	
	private void init(boolean onStart) {
		Configuration config = ServiceBusConfiguration.configureWithSASAuthentication(namespace, keyName, key, rootUri);
		service = ServiceBusService.create(config);
		
		try {
			ListQueuesResult qresult = service.listQueues();
			makeAddQueueAction(qresult.getItems(), onStart);
			ListTopicsResult tresult = service.listTopics();
			makeAddTopicAction(tresult.getItems(), onStart);
			statNode.setValue(DSElement.make("Connected"));
		} catch (ServiceException e) {
			statNode.setValue(DSElement.make("Service Exception"));
		}
		
		
		makeEditAction(onStart);
	}
	
	private void makeAddQueueAction(List<QueueInfo> queues, boolean onStart) {
		List<String> queueNames = new ArrayList<String>();
		for (QueueInfo qInfo: queues) {
			queueNames.add(qInfo.getPath());
		}
		MyDSActionNode act = new MyDSActionNode(DSPermission.READ, new InvokeHandler() {
			@Override
			public ActionResult handle(DSMap parameters, InboundInvokeRequestHandle reqHandle) {
				addQueue(parameters);
				return new ActionResult() {};
			}
		});
		act.addParameter("Queue_Name", null, MyValueType.enumOf(queueNames), null, null);
		addChild("Add_Queue", act, onStart);
	}

	private void makeAddTopicAction(List<TopicInfo> topics, boolean onStart) {
		List<String> topicNames = new ArrayList<String>();
		for (TopicInfo tInfo: topics) {
			topicNames.add(tInfo.getPath());
		}
		MyDSActionNode act = new MyDSActionNode(DSPermission.READ, new InvokeHandler() {
			@Override
			public ActionResult handle(DSMap parameters, InboundInvokeRequestHandle reqHandle) {
				addTopic(parameters);
				return new ActionResult() {};
			}
		});
		act.addParameter("Topic_Name", null, MyValueType.enumOf(topicNames), null, null);
		addChild("Add_Topic", act, onStart);
	}
	
	private void makeRemoveAction(boolean onStart) {
		MyDSActionNode act = new MyDSActionNode(DSPermission.READ, new InvokeHandler() {
			@Override
			public ActionResult handle(DSMap parameters, InboundInvokeRequestHandle reqHandle) {
				delete();
				return new ActionResult() {};
			}
		});
		addChild("Remove", act, onStart);
	}
	
	private void makeEditAction(boolean onStart) {
		MyDSActionNode act = new MyDSActionNode(DSPermission.READ, new InvokeHandler() {
			@Override
			public ActionResult handle(DSMap parameters, InboundInvokeRequestHandle reqHandle) {
				edit(parameters);
				return new ActionResult() {};
			}
    	});
    	act.addParameter("Namespace", DSElement.make(namespace), null, null, null);
    	act.addParameter("SAS_Key_Name", DSElement.make(keyName), null, null, null);
    	act.addParameter("SAS_Key", DSElement.make(key), null, null, null);
    	act.addParameter("Service_Bus_Root_Uri", DSElement.make(rootUri), null, null, null);
    	addChild("Edit", act, onStart);
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
	
	private void makeCreateQueueAction(boolean onStart) {
		MyDSActionNode act = new MyDSActionNode(DSPermission.READ, new InvokeHandler() {
			@Override
			public ActionResult handle(DSMap parameters, InboundInvokeRequestHandle reqHandle) {
				createQueue(parameters);
				return new ActionResult() {};
			}
    	});
		act.addParameter("Name", null, MyValueType.STRING, null, null);
		addChild("Create_Queue", act, onStart);
	}

	private void makeCreateTopicAction(boolean onStart) {
		MyDSActionNode act = new MyDSActionNode(DSPermission.READ, new InvokeHandler() {
			@Override
			public ActionResult handle(DSMap parameters, InboundInvokeRequestHandle reqHandle) {
				createTopic(parameters);
				return new ActionResult() {};
			}
    	});
		act.addParameter("Name", null, MyValueType.STRING, null, null);
		addChild("Create_Topic", act, onStart);
	}
	
	
	private void edit(DSMap parameters) {
		namespace = parameters.getString("Namespace");
    	keyName = parameters.getString("SAS_Key_Name");
    	key = parameters.getString("SAS_Key");
    	rootUri = parameters.getString("Service_Bus_Root_Uri");
    	init(false);
	}
	
	private void addQueue(DSMap parameters) {
		String name = parameters.getString("Queue_Name");
		QueueInfo qInfo = new QueueInfo(name);
		queuesNode.addChild(qInfo.getPath(), new QueueNode(qInfo, this), false);
	}
	
	private void addTopic(DSMap parameters) {
		String name = parameters.getString("Topic_Name");
		TopicInfo tInfo = new TopicInfo(name);
		topicsNode.addChild(tInfo.getPath(), new TopicNode(tInfo, this), false);
	}
	
	private void createQueue(DSMap parameters) {
		String name = parameters.getString("Name");
		QueueInfo queueInfo = new QueueInfo(name);
		try {
			service.createQueue(queueInfo);
			queuesNode.addChild(queueInfo.getPath(), new QueueNode(queueInfo, this), false);
		} catch (ServiceException e) {
			// TODO Send Error
			warn("Error Creating Queue: " + e);
		}
	}
	
	private void createTopic(DSMap parameters) {
		String name = parameters.getString("Name");
		TopicInfo topicInfo = new TopicInfo(name);
		try {
			service.createTopic(topicInfo);
			topicsNode.addChild(topicInfo.getPath(), new TopicNode(topicInfo, this), false);
		} catch (ServiceException e) {
			// TODO Send Error
			warn("Error Creating Topic: " + e);
		}
	}
	
	@Override
	public void getMetaData(DSMap metaData) {
		super.getMetaData(metaData);
		metaData.put("Namespace", namespace);
		metaData.put("SAS_Key_Name", keyName);
		metaData.put("SAS_Key", key);
		metaData.put("Service_Bus_Root_Uri", rootUri);
	}

}
