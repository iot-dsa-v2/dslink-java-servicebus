package org.iot.dsa.servicebus;

import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.models.BrokeredMessage;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMessageOptions;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMode;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map.Entry;
import org.iot.dsa.DSRuntime;
import org.iot.dsa.DSRuntime.Timer;
import org.iot.dsa.dslink.Action.ResultsType;
import org.iot.dsa.dslink.ActionResults;
import org.iot.dsa.dslink.DSRequestException;
import org.iot.dsa.node.DSBool;
import org.iot.dsa.node.DSIValue;
import org.iot.dsa.node.DSInfo;
import org.iot.dsa.node.DSList;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSValueType;
import org.iot.dsa.node.action.DSActionResults;
import org.iot.dsa.node.action.DSIActionRequest;
import org.iot.dsa.node.action.DSIAction;
import org.iot.dsa.node.action.DSAction;

/**
 * The base class for nodes that have a "Recieve Messages" action, namely QueueNode and
 * SubscriptionNode. Handles the streaming of messages from Azure Service Bus into DSA.
 *
 * @author Daniel Shapiro
 */
public abstract class ReceiverNode extends RemovableNode {

    @SuppressWarnings("serial")
    private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ") {
        public Date parse(String source, ParsePosition pos) {
            return super.parse(source.replaceFirst(":(?=[0-9]{2}$)", ""), pos);
        }
    };

    @Override
    protected void declareDefaults() {
        super.declareDefaults();
        declareDefault("Recieve Messages", makeReadAction());
    }

    private DSAction makeReadAction() {
        DSAction act = new DSAction() {
            @Override
            public ActionResults invoke(DSIActionRequest req) {
                return ((ReceiverNode) req.getTarget()).invokeReceive(req);
            }
        };
        act.addParameter("Use Peek-Lock", DSBool.TRUE, null);
        act.setResultsType(ResultsType.STREAM);
        return act;
    }

    protected ActionResults invokeReceive(final DSIActionRequest req) {
        DSMap parameters = req.getParameters();
        boolean peekLock = parameters.getBoolean("Use Peek-Lock");
        final ReceiveMessageOptions opts = ReceiveMessageOptions.DEFAULT;
        opts.setReceiveMode(peekLock ? ReceiveMode.PEEK_LOCK : ReceiveMode.RECEIVE_AND_DELETE);
        Receiver receiver = new Receiver(req, opts);
        final Timer t = DSRuntime.run(receiver, System.currentTimeMillis() + 1000, 1000);
        final DSActionResults ret = new DSActionResults(req) {

            @Override
            public int getColumnCount() {
                return 4;
            }

            @Override
            public void getColumnMetadata(int col, DSMap bucket) {
                switch (col) {
                    case 0:
                        bucket.putAll(Util.makeColumn("ID", DSValueType.STRING));
                        break;
                    case 1:
                        bucket.putAll(Util.makeColumn("Timestamp", DSValueType.STRING));
                        break;
                    case 2:
                        bucket.putAll(Util.makeColumn("Body", DSValueType.STRING));
                        break;
                    default:
                        bucket.putAll(Util.makeColumn("Properties", DSValueType.MAP));
                        break;
                }
            }

            @Override
            public void onClose() {
                t.cancel();
            }
        };

        receiver.setActionResults(ret);
        return ret;
    }

    private class Receiver implements Runnable {

        private DSIActionRequest req;
        private ReceiveMessageOptions opts;
        private DSActionResults results;

        public Receiver(DSIActionRequest req, ReceiveMessageOptions opts) {
            this.req = req;
            this.opts = opts;
        }

        @Override
        public void run() {
            while (req.isOpen()) {
                try {
                    BrokeredMessage message = receiveMessage(opts);
                    if (message == null) {
                        break;
                    } else if (message.getMessageId() != null) {
                        String id = message.getMessageId();
                        byte[] b = new byte[200];
                        StringBuilder s = new StringBuilder();
                        try {
                            int numRead = message.getBody().read(b);
                            while (-1 != numRead) {
                                s.append(new String(b));
                                numRead = message.getBody().read(b);
                            }
                        } catch (IOException e) {
                            warn(e);
                        }
                        String date = dateFormat.format(message.getDate());
                        DSMap properties = new DSMap();
                        for (Entry<String, Object> entry : message.getProperties().entrySet()) {
                            Util.putInMap(properties, entry.getKey(), entry.getValue());
                        }
                        results.addResults(new DSList().add(id).add(date).add(s.toString().trim())
                                                    .add(properties));
                        if (opts.isPeekLock()) {
                            deleteMessage(message);
                        }
                    }
                } catch (ServiceException e) {
                    warn("Error Receiving Message: " + e);
                    req.close(e);
                }
            }
        }

        void setActionResults(DSActionResults results) {
            this.results = results;
        }
    }

    public abstract BrokeredMessage receiveMessage(ReceiveMessageOptions opts)
            throws ServiceException;

    public abstract void deleteMessage(BrokeredMessage message);

}
