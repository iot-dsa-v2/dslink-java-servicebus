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
import org.iot.dsa.dslink.DSRequestException;
import org.iot.dsa.node.DSBool;
import org.iot.dsa.node.DSInfo;
import org.iot.dsa.node.DSList;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSValueType;
import org.iot.dsa.node.action.ActionInvocation;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.node.action.ActionResultSpec;
import org.iot.dsa.node.action.ActionSpec;
import org.iot.dsa.node.action.ActionTable;
import org.iot.dsa.node.action.DSAction;
import org.iot.dsa.node.action.ActionSpec.ResultType;

import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.models.BrokeredMessage;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMessageOptions;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMode;

public abstract class ReceiverNode extends RemovableNode {
	@SuppressWarnings("serial")
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ") { 
	    public Date parse(String source, ParsePosition pos) {    
	        return super.parse(source.replaceFirst(":(?=[0-9]{2}$)",""),pos);
	    }
	};
	
	@Override
	protected void declareDefaults() {
		super.declareDefaults();
		declareDefault("Recieve_Messages", makeReadAction());
	}
	
	private DSAction makeReadAction() {
		DSAction act = new DSAction() {
			@Override
			public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
				 return ((ReceiverNode) info.getParent()).invokeReceive(info, invocation);
			}
		};
		act.addParameter("Use_Peek-Lock", DSBool.TRUE, null);
		act.setResultType(ResultType.STREAM_TABLE);
		return act;
	}
	
	protected ActionResult invokeReceive(final DSInfo info, ActionInvocation invocation) {
		DSMap parameters = invocation.getParameters();
		boolean peekLock = parameters.getBoolean("Use_Peek-Lock");
		final ReceiveMessageOptions opts = ReceiveMessageOptions.DEFAULT;
		opts.setReceiveMode(peekLock ? ReceiveMode.PEEK_LOCK : ReceiveMode.RECEIVE_AND_DELETE);
		Receiver runnable = new Receiver(invocation, opts);
		final Timer t = DSRuntime.run(runnable, System.currentTimeMillis() + 1000, 1000);
		
		return new ActionTable() {
			@Override
			public Iterator<DSList> getRows() {
				return new ArrayList<DSList>().iterator();
			}
			@Override
			public Iterator<ActionResultSpec> getColumns() {
				ActionResultSpec c1 = new Util.MyColumn("ID", DSValueType.STRING);
				ActionResultSpec c2 = new Util.MyColumn("Timestamp", DSValueType.STRING);
				ActionResultSpec c3 = new Util.MyColumn("Body", DSValueType.STRING);	
				List<ActionResultSpec> colList = new ArrayList<ActionResultSpec>();
				colList.add(c1);
				colList.add(c2);
				colList.add(c3);
				return colList.iterator();
			}
			@Override
			public ActionSpec getAction() {
				return info.getAction();
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
			while(invocation.isOpen()) {
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
						invocation.send(new DSList().add(id).add(date).add(s.toString().trim()));
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

	public abstract BrokeredMessage receiveMessage(ReceiveMessageOptions opts) throws ServiceException;
	
	public abstract void deleteMessage(BrokeredMessage message);
	
}
