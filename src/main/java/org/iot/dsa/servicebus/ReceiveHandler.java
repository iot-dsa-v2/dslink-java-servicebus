package org.iot.dsa.servicebus;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.iot.dsa.DSRuntime;
import org.iot.dsa.DSRuntime.Timer;
import org.iot.dsa.logging.DSLogger;
import org.iot.dsa.node.DSList;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSValueType;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.node.action.ActionResultSpec;
import org.iot.dsa.node.action.ActionTable;
import org.iot.dsa.servicebus.node.MyColumn;
import org.iot.dsa.servicebus.node.MyDSActionNode.InboundInvokeRequestHandle;
import org.iot.dsa.servicebus.node.MyDSActionNode.InvokeHandler;

import com.microsoft.windowsazure.services.servicebus.models.BrokeredMessage;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMessageOptions;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMode;

public class ReceiveHandler extends DSLogger implements InvokeHandler {
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ") { 
	    public Date parse(String source, ParsePosition pos) {    
	        return super.parse(source.replaceFirst(":(?=[0-9]{2}$)",""),pos);
	    }
	};
	
	private ReceiverNode receiverNode;

	public ReceiveHandler(ReceiverNode receiverNode) {
		this.receiverNode = receiverNode;
	}

	@Override
	public ActionResult handle(DSMap parameters, InboundInvokeRequestHandle reqHandle) {
		boolean peekLock = parameters.getBoolean("Use_Peek-Lock");
		final ReceiveMessageOptions opts = ReceiveMessageOptions.DEFAULT;
		opts.setReceiveMode(peekLock ? ReceiveMode.PEEK_LOCK : ReceiveMode.RECEIVE_AND_DELETE);
		Receiver runnable = new Receiver(reqHandle, opts);
		Timer t = DSRuntime.run(runnable, System.currentTimeMillis() + 1000, 1000);
		runnable.setTimer(t);
		
		return new ActionTable() {
			@Override
			public Iterator<DSList> getRows() {
				return new ArrayList<DSList>().iterator();
			}
			@Override
			public Iterator<ActionResultSpec> getColumns() {
				ActionResultSpec c1 = new MyColumn("ID", DSValueType.STRING);
				ActionResultSpec c2 = new MyColumn("Timestamp", DSValueType.STRING);
				ActionResultSpec c3 = new MyColumn("Body", DSValueType.STRING);	
				List<ActionResultSpec> colList = new ArrayList<ActionResultSpec>();
				colList.add(c1);
				colList.add(c2);
				colList.add(c3);
				return colList.iterator();
			}
		};
	}
	
	private class Receiver implements Runnable {
		private InboundInvokeRequestHandle reqHandle;
		private ReceiveMessageOptions opts;
		private Timer myTimer;
		
		public Receiver(InboundInvokeRequestHandle reqHandle, ReceiveMessageOptions opts) {
			super();
			this.reqHandle = reqHandle;
			this.opts = opts;
		}
		
		public void setTimer(Timer t) {
			this.myTimer = t;
		}

		@Override
		public void run() {
			while(!reqHandle.isClosed()) {
				BrokeredMessage message = receiverNode.receiveMessage(opts);
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
					reqHandle.send(new DSList().add(id).add(date).add(s.toString().trim()));
					if (opts.isPeekLock()) {
						receiverNode.deleteMessage(message);
					}
				}
			}
			if (reqHandle.isClosed() && myTimer != null) {
				myTimer.cancel();
			}
		}
		
	}
}
