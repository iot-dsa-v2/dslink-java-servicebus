package org.iot.dsa.servicebus;

import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.ServiceBusConfiguration;
import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;
import com.microsoft.windowsazure.services.servicebus.ServiceBusService;
import com.microsoft.windowsazure.services.servicebus.models.ListQueuesResult;
import com.microsoft.windowsazure.services.servicebus.models.ListTopicsResult;
import com.microsoft.windowsazure.services.servicebus.models.QueueInfo;
import com.microsoft.windowsazure.services.servicebus.models.TopicInfo;
import java.util.ArrayList;
import java.util.List;
import org.iot.dsa.dslink.ActionResults;
import org.iot.dsa.dslink.DSRequestException;
import org.iot.dsa.node.DSIObject;
import org.iot.dsa.node.DSInfo;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSNode;
import org.iot.dsa.node.DSString;
import org.iot.dsa.node.action.DSAction;
import org.iot.dsa.node.action.DSIActionRequest;
import org.iot.dsa.servicebus.Util.MyValueType;

/**
 * An instance of this node represents a specific Azure Service Bus Namespace.
 *
 * @author Daniel Shapiro
 */
public class ServiceBusNode extends RemovableNode {

    private DSInfo addQueue;
    private DSInfo addTopic;
    private DSInfo createQueue = getInfo("Create Queue");
    private DSInfo createTopic = getInfo("Create Topic");
    private DSInfo edit;
    private String key;
    private String keyName;
    private String namespace;
    private DSNode queuesNode;
    private DSInfo refresh = getInfo("Refresh");
    private String rootUri;
    private ServiceBusContract service;
    private DSInfo status = getInfo("Status");
    private DSNode topicsNode;


    /**
     * Do not use
     */
    public ServiceBusNode() {
        super();
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
    public ActionResults invoke(DSIActionRequest req) {
        DSInfo actionInfo = req.getActionInfo();
        DSInfo targetInfo = req.getTargetInfo();
        if (actionInfo == this.refresh) {
            init();
        } else if (actionInfo == this.createQueue) {
            createQueue(req.getParameters());
        } else if (actionInfo == this.createTopic) {
            createTopic(req.getParameters());
        } else if (actionInfo == this.addQueue) {
            addQueue(req.getParameters());
        } else if (actionInfo == this.addTopic) {
            addTopic(req.getParameters());
        } else if (actionInfo == this.edit) {
            edit(req.getParameters());
        }
        return null;
    }

    @Override
    protected void declareDefaults() {
        super.declareDefaults();
        declareDefault("Queues", new DSNode());
        declareDefault("Topics", new DSNode());

        declareDefault("Refresh", DSAction.DEFAULT);
        declareDefault("Create Queue", makeCreateAction());
        declareDefault("Create Topic", makeCreateAction());
    }

    @Override
    protected void onStable() {
        status = add("STATUS", DSString.valueOf("Connecting")).setTransient(true);
        queuesNode = getNode("Queues");
        topicsNode = getNode("Topics");
        init();
    }

    @Override
    protected void onStarted() {
        if (namespace == null) {
            DSIObject ns = get("Namespace");
            namespace = ns instanceof DSString ? ns.toString() : "";
        }
        if (keyName == null) {
            DSIObject kn = get("SAS Key Name");
            keyName = kn instanceof DSString ? kn.toString() : "RootManageSharedAccessKey";
        }
        if (key == null) {
            DSIObject k = get("SAS Key");
            key = k instanceof DSString ? k.toString() : "";
        }
        if (rootUri == null) {
            DSIObject ru = get("Service Bus Root Uri");
            rootUri = ru instanceof DSString ? ru.toString() : ".servicebus.windows.net";
        }
    }

    private void addQueue(DSMap parameters) {
        String name = parameters.getString("Queue Name");
        QueueInfo qInfo = new QueueInfo(name);
        queuesNode.add(qInfo.getPath(), new QueueNode(qInfo, this));
    }

    private void addTopic(DSMap parameters) {
        String name = parameters.getString("Topic Name");
        TopicInfo tInfo = new TopicInfo(name);
        topicsNode.add(tInfo.getPath(), new TopicNode(tInfo, this));
    }

    private void createQueue(DSMap parameters) {
        String name = parameters.getString("Name");
        QueueInfo queueInfo = new QueueInfo(name);
        try {
            service.createQueue(queueInfo);
            queuesNode.add(queueInfo.getPath(), new QueueNode(queueInfo, this));
        } catch (ServiceException e) {
            warn("Error Creating Queue: " + e);
            throw new DSRequestException(e.getMessage());
        }
    }

    private void createTopic(DSMap parameters) {
        String name = parameters.getString("Name");
        TopicInfo topicInfo = new TopicInfo(name);
        try {
            service.createTopic(topicInfo);
            topicsNode.add(topicInfo.getPath(), new TopicNode(topicInfo, this));
        } catch (ServiceException e) {
            warn("Error Creating Topic: " + e);
            throw new DSRequestException(e.getMessage());
        }
    }

    private void edit(DSMap parameters) {
        namespace = parameters.getString("Namespace");
        keyName = parameters.getString("SAS Key Name");
        key = parameters.getString("SAS Key");
        rootUri = parameters.getString("Service Bus Root Uri");
        init();
    }

    private void init() {
        put("Namespace", DSString.valueOf(namespace)).setReadOnly(true);
        put("SAS Key Name", DSString.valueOf(keyName)).setReadOnly(true);
        put("SAS Key", DSString.valueOf(key)).setReadOnly(true);
        put("Service Bus Root Uri", DSString.valueOf(rootUri)).setReadOnly(true);
        Configuration config = ServiceBusConfiguration.configureWithSASAuthentication(namespace,
                                                                                      keyName, key,
                                                                                      rootUri);
        service = ServiceBusService.create(config);

        try {
            ListQueuesResult qresult = service.listQueues();
            addQueue = put("Add Queue", makeAddQueueAction(qresult.getItems())).setTransient(true);

            ListTopicsResult tresult = service.listTopics();
            addTopic = put("Add Topic", makeAddTopicAction(tresult.getItems())).setTransient(true);

            put(status, DSString.valueOf("Connected"));
        } catch (ServiceException e) {
            put(status, DSString.valueOf("Service Exception"));
        }

        edit = put("Edit", makeEditAction()).setTransient(true);

        for (DSInfo info : topicsNode) {
            DSIObject n = info.get();
            if (n instanceof TopicNode) {
                ((TopicNode) n).init();
            }
        }
    }

    private DSAction makeAddQueueAction(List<QueueInfo> queues) {
        List<String> queueNames = new ArrayList<String>();
        for (QueueInfo qInfo : queues) {
            queueNames.add(qInfo.getPath());
        }
        DSAction act = new DSAction();
        act.addParameter(
                Util.makeParameter("Queue Name", null, MyValueType.enumOf(queueNames), null, null));
        return act;
    }

    private DSAction makeAddTopicAction(List<TopicInfo> topics) {
        List<String> topicNames = new ArrayList<String>();
        for (TopicInfo tInfo : topics) {
            topicNames.add(tInfo.getPath());
        }
        DSAction act = new DSAction();
        act.addParameter(
                Util.makeParameter("Topic Name", null, MyValueType.enumOf(topicNames), null, null));
        return act;
    }

    private DSAction makeCreateAction() {
        DSAction act = new DSAction();
        act.addParameter("Name", DSString.NULL, null);
        return act;
    }

    private DSAction makeEditAction() {
        DSAction act = new DSAction();
        act.addDefaultParameter("Namespace", DSString.valueOf(namespace), null);
        act.addDefaultParameter("SAS Key Name", DSString.valueOf(keyName), null);
        act.addDefaultParameter("SAS Key", DSString.valueOf(key), null);
        act.addDefaultParameter("Service Bus Root Uri", DSString.valueOf(rootUri), null);
        return act;
    }

}
