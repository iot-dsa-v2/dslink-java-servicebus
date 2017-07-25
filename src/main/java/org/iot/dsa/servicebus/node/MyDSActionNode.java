package org.iot.dsa.servicebus.node;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.iot.dsa.dslink.responder.InboundInvokeRequest;
import org.iot.dsa.dslink.responder.OutboundInvokeResponse;
import org.iot.dsa.node.DSElement;
import org.iot.dsa.node.DSList;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSValueType;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.node.action.ActionResultSpec;
import org.iot.dsa.node.action.ActionSpec;
import org.iot.dsa.security.DSPermission;

public class MyDSActionNode extends MyDSNode implements ActionSpec {
	
	private final List<DSMap> parameters = new ArrayList<DSMap>();
	private final List<ActionResultSpec> columns = new ArrayList<ActionResultSpec>();
	private ResultType resType = ResultType.VOID;
	private DSPermission invokable;
	private InvokeHandler handler;
	
	public MyDSActionNode() {
	}
	
	public MyDSActionNode(DSPermission invokePermission, InvokeHandler handler) {
		this.invokable = invokePermission;
		this.handler = handler;
	}

	public void setInvokable(DSPermission invokable) {
		this.invokable = invokable;
	}

	public void setHandler(InvokeHandler handler) {
		this.handler = handler;
	}
	
	public void addColumn(String name, DSValueType type) {
		if (name == null || type == null) {
			return;
		}
		MyColumn col = new MyColumn(name, type);
		columns.add(col);
	}

	public void addParameter(String name, DSElement def, MyValueType type, String description, String placeholder) {
		if (name == null || (def == null && type == null)) {
			return;
		}
		DSMap param = new DSMap();
		param.put("name", name);
		if (def != null) {
			param.put("default", def);
		}
		if (type != null) {
			param.put("type", type.toString());
		}
		if (description != null) {
			param.put("description", description);
		}
		if (placeholder != null) {
			param.put("placeholder", placeholder);
		}
		
		parameters.add(param);
	}

	@Override
	public Iterator<DSMap> getParameters() {
		return parameters.iterator();
	}

	public void setResultType(ResultType resType) {
		this.resType = resType;
	}
	
	@Override
	public ResultType getResultType() {
		return resType;
	}

	@Override
	public DSPermission getPermission() {
		return invokable;
	}
	
	@Override
	public OutboundInvokeResponse onInvoke(InboundInvokeRequest req, String path) {
		DSPermission permission = req.getPermission();
		if (invokable != null && permission.getLevel() >= invokable.getLevel() && handler != null) {
			final InboundInvokeRequestHandle handle = new InboundInvokeRequestHandle(req);
			final ActionResult result = handler.handle(req.getParameters(), handle );
			final ActionSpec act = this;
			return new OutboundInvokeResponse() {

				@Override
				public ActionSpec getAction() {
					return act;
				}

				@Override
				public ActionResult getResult() {
					return result;
				}

				@Override
				public void onClose() {
					handle.close();
				}				
			};
		}
		return null;
	}
	
	public static class InboundInvokeRequestHandle {
		InboundInvokeRequest req;

		public InboundInvokeRequestHandle(InboundInvokeRequest req) {
			this.req = req;
		}
		
		public void close() {
			this.req = null;
		}
		
		public boolean isClosed() {
			return req == null;
		}
		
		public void send(DSList row) {
			if (req != null) {
				req.send(row);
			}
		}
		
		public void insert(int index, DSList... rows) {
			if (req != null) {
				req.insert(index, rows);
			}
		}
		
		public void replace(int index, int len, DSList... rows) {
			if (req != null) {
				req.replace(index, len, rows);
			}
		}
		
		public void clearAllRows() {
			if (req != null) {
				req.clearAllRows();
			}
		}
	}
	
	@Override
	public void getMetaData(DSMap metaData) {
//		metaData.put("columns", columns.copy());
	}

	@Override
	public Iterator<ActionResultSpec> getResultSpecs() {
		return columns.iterator();
	}

}
