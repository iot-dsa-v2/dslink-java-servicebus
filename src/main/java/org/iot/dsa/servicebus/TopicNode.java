package org.iot.dsa.servicebus;

import java.util.ArrayList;
import java.util.List;
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
import org.iot.dsa.servicebus.Util.MyValueType;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;
import com.microsoft.windowsazure.services.servicebus.models.BrokeredMessage;
import com.microsoft.windowsazure.services.servicebus.models.ListSubscriptionsResult;
import com.microsoft.windowsazure.services.servicebus.models.SubscriptionInfo;
import com.microsoft.windowsazure.services.servicebus.models.TopicInfo;

/**
 * An instance of this node represents a specific Azure Service Bus Topic.
 *
 * @author Daniel Shapiro
 */
public class TopicNode extends RemovableNode {

    private TopicInfo info;
    private ServiceBusNode serviceNode;

    /**
     * Do not use
     */
    public TopicNode() {
        super();
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
    protected void declareDefaults() {
        super.declareDefaults();
        declareDefault("Send Message", makeSendAction());
        declareDefault("Create Subscription", makeCreateSubscriptionAction());
        declareDefault("Refresh", makeRefreshAction());
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
            info = new TopicInfo(getName());
        }
        if (getService() != null) {
            init();
        }
    }

    void init() {
        try {
            ListSubscriptionsResult result = getService().listSubscriptions(info.getPath());
            put("Add Subscription", makeAddSubscriptionAction(result.getItems()))
                    .setTransient(true);
        } catch (ServiceException e) {
            warn("Error listing subscriptions: " + e);
        }
    }

    private DSAction makeRefreshAction() {
        DSAction act = new DSAction() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                ((TopicNode) info.getParent()).init();
                return null;
            }
        };
        return act;
    }

    private DSAction makeAddSubscriptionAction(List<SubscriptionInfo> subscriptions) {
        List<String> subNames = new ArrayList<String>();
        for (SubscriptionInfo sInfo : subscriptions) {
            subNames.add(sInfo.getName());
        }
        DSAction act = new DSAction() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                ((TopicNode) info.getParent()).addSubscription(invocation.getParameters());
                return null;
            }
        };
        act.addParameter(Util.makeParameter("Subscription Name", null, MyValueType.enumOf(subNames),
                null, null));
        return act;
    }

    private DSAction makeSendAction() {
        DSAction act = new DSAction() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                ((TopicNode) info.getParent()).handleSend(invocation.getParameters());
                return null;
            }
        };
        act.addParameter("Message", DSString.NULL, null);
        act.addDefaultParameter("Properties", new DSMap(), null);
        return act;
    }

    private DSAction makeCreateSubscriptionAction() {
        DSAction act = new DSAction() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                ((TopicNode) info.getParent()).createSubscription(invocation.getParameters());
                return null;
            }
        };
        act.addParameter("Name", DSString.NULL, null);
        return act;
    }

    @Override
    protected DSAction makeRemoveAction() {
        DSAction act = new DSAction() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                ((TopicNode) info.getParent()).handleDelete(invocation.getParameters());
                return null;
            }
        };
        act.addDefaultParameter("Delete From Namespace", DSBool.FALSE, null);
        return act;
    }


    private void addSubscription(DSMap parameters) {
        String name = parameters.getString("Subscription Name");
        SubscriptionInfo sInfo = new SubscriptionInfo(name);
        add(sInfo.getName(), new SubscriptionNode(sInfo, this));
    }

    private void handleSend(DSMap parameters) {
        String messageText = parameters.getString("Message");
        DSMap properties = parameters.getMap("Properties");
        BrokeredMessage message = new BrokeredMessage(messageText);
        for (Entry entry : properties) {
            message.setProperty(entry.getKey(), entry.getValue().toString());
        }
        try {
            getService().sendTopicMessage(info.getPath(), message);
        } catch (ServiceException e) {
            warn("Error Sending Message: " + e);
            throw new DSRequestException(e.getMessage());
        }
    }

    private void createSubscription(DSMap parameters) {
        String name = parameters.getString("Name");
        SubscriptionInfo sinfo = new SubscriptionInfo(name);
        try {
            getService().createSubscription(info.getPath(), sinfo);
            add(sinfo.getName(), new SubscriptionNode(sinfo, this));
        } catch (ServiceException e) {
            warn("Error Creating Subscription: " + e);
            throw new DSRequestException(e.getMessage());
        }
    }

    private void handleDelete(DSMap parameters) {
        if (parameters.get("Delete From Namespace", false)) {
            try {
                getService().deleteTopic(info.getPath());
            } catch (ServiceException e) {
                warn("Error Deleting Topic: " + e);
                throw new DSRequestException(e.getMessage());
            }
        }
        delete();
    }


}
