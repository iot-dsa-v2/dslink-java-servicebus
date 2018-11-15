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
import org.iot.dsa.dslink.DSRequestException;
import org.iot.dsa.node.DSBool;
import org.iot.dsa.node.DSIValue;
import org.iot.dsa.node.DSInfo;
import org.iot.dsa.node.DSList;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSValueType;
import org.iot.dsa.node.action.ActionInvocation;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.node.action.ActionSpec;
import org.iot.dsa.node.action.ActionSpec.ResultType;
import org.iot.dsa.node.action.ActionTable;
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
        DSAction act = new DSAction.Parameterless() {
            @Override
            public ActionResult invoke(DSInfo target, ActionInvocation invocation) {
                return ((ReceiverNode) target.get()).invokeReceive(this, invocation);
            }
        };
        act.addParameter("Use Peek-Lock", DSBool.TRUE, null);
        act.setResultType(ResultType.STREAM_TABLE);
        return act;
    }

    protected ActionResult invokeReceive(final DSAction action, ActionInvocation invocation) {
        DSMap parameters = invocation.getParameters();
        boolean peekLock = parameters.getBoolean("Use Peek-Lock");
        final ReceiveMessageOptions opts = ReceiveMessageOptions.DEFAULT;
        opts.setReceiveMode(peekLock ? ReceiveMode.PEEK_LOCK : ReceiveMode.RECEIVE_AND_DELETE);
        Receiver runnable = new Receiver(invocation, opts);
        final Timer t = DSRuntime.run(runnable, System.currentTimeMillis() + 1000, 1000);

        return new ActionTable() {

            @Override
            public ActionSpec getAction() {
                return action;
            }

            @Override
            public int getColumnCount() {
                return 4;
            }

            @Override
            public void getMetadata(int col, DSMap bucket) {
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
            public DSIValue getValue(int col) {
                return null;
            }

            @Override
            public boolean next() {
                return false;
            }

            @Override
            public void onClose() {
                t.cancel();
            }
        };

    }

    private class Receiver implements Runnable {

        private ActionInvocation invocation;
        private ReceiveMessageOptions opts;

        public Receiver(ActionInvocation invocation, ReceiveMessageOptions opts) {
            super();
            this.invocation = invocation;
            this.opts = opts;
        }

        @Override
        public void run() {
            while (invocation.isOpen()) {
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
                        invocation.send(new DSList().add(id).add(date).add(s.toString().trim())
                                                    .add(properties));
                        if (opts.isPeekLock()) {
                            deleteMessage(message);
                        }
                    }
                } catch (ServiceException e) {
                    warn("Error Receiving Message: " + e);
                    invocation.close(new DSRequestException(e.getMessage()));
                }
            }
        }
    }

    public abstract BrokeredMessage receiveMessage(ReceiveMessageOptions opts)
            throws ServiceException;

    public abstract void deleteMessage(BrokeredMessage message);

}
