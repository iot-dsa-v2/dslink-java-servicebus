package org.iot.dsa.servicebus;

import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.models.BrokeredMessage;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMessageOptions;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveSubscriptionMessageResult;
import com.microsoft.windowsazure.services.servicebus.models.SubscriptionInfo;
import org.iot.dsa.dslink.ActionResults;
import org.iot.dsa.dslink.DSRequestException;
import org.iot.dsa.node.DSBool;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSNode;
import org.iot.dsa.node.action.DSAction;
import org.iot.dsa.node.action.DSIActionRequest;

/**
 * An instance of this node represents a specific Subscription for an Azure Service Bus Topic.
 *
 * @author Daniel Shapiro
 */
public class SubscriptionNode extends ReceiverNode {

    private SubscriptionInfo info;
    private TopicNode topicNode;

    /**
     * Do not use
     */
    public SubscriptionNode() {
        super();
    }

    public SubscriptionNode(SubscriptionInfo info, TopicNode topicNode) {
        super();
        this.info = info;
        this.topicNode = topicNode;
    }

    @Override
    public void deleteMessage(BrokeredMessage message) {
        try {
            topicNode.getService().deleteMessage(message);
        } catch (ServiceException e) {
            warn("Error Deleting Message: " + e);
        }
    }

    @Override
    public BrokeredMessage receiveMessage(ReceiveMessageOptions opts) throws ServiceException {
        ReceiveSubscriptionMessageResult resultSubMsg;
        resultSubMsg = topicNode.getService().receiveSubscriptionMessage(topicNode.getTopicName(),
                                                                         info.getName(), opts);
        return resultSubMsg.getValue();
    }

    @Override
    protected DSAction makeRemoveAction() {
        DSAction act = new DSAction() {
            @Override
            public ActionResults invoke(DSIActionRequest req) {
                ((SubscriptionNode) req.getTarget()).handleDelete(req.getParameters());
                return null;
            }
        };
        act.addDefaultParameter("Delete From Namespace", DSBool.FALSE, null);
        return act;
    }

    @Override
    protected void onStable() {
        if (topicNode == null) {
            DSNode n = getParent();
            if (n instanceof TopicNode) {
                topicNode = ((TopicNode) n);
            }
        }
        if (info == null) {
            info = new SubscriptionInfo(getName());
        }
    }

    private void handleDelete(DSMap parameters) {
        if (parameters.get("Delete From Namespace", false)) {
            try {
                topicNode.getService().deleteSubscription(topicNode.getTopicName(), info.getName());
            } catch (ServiceException e) {
                warn("Error Deleting Subscription: " + e);
                throw new DSRequestException(e.getMessage());
            }
        }
        delete();
    }

}
