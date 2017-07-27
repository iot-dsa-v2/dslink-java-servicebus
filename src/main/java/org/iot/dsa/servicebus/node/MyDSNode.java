package org.iot.dsa.servicebus.node;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.glassfish.grizzly.utils.Pair;
import org.iot.dsa.dslink.DSInvalidPathException;
import org.iot.dsa.dslink.DSRequestException;
import org.iot.dsa.dslink.responder.ApiNode;
import org.iot.dsa.dslink.responder.ApiObject;
import org.iot.dsa.dslink.responder.InboundInvokeRequest;
import org.iot.dsa.dslink.responder.InboundListRequest;
import org.iot.dsa.dslink.responder.InboundSubscribeRequest;
import org.iot.dsa.dslink.responder.OutboundInvokeResponse;
import org.iot.dsa.dslink.responder.OutboundListResponse;
import org.iot.dsa.dslink.responder.SubscriptionCloseHandler;
import org.iot.dsa.node.DSContainer;
import org.iot.dsa.node.DSElement;
import org.iot.dsa.node.DSIObject;
import org.iot.dsa.node.DSInfo;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSNode;
import org.iot.dsa.security.DSPermission;

public class MyDSNode extends DSNode implements ApiNode {
	
	private InboundListRequest listHandle = null;
	private String cachedName = null;
	
	
	
	public OutboundInvokeResponse onInvoke(InboundInvokeRequest req, String path) {
		if (path.isEmpty() || path.equals("/")) {
			throw new DSRequestException("Path not invokable");
		} else {
			Pair<MyDSNode, String> pair = traverseDown(path);
			MyDSNode child = pair.getFirst();
			String restOfPath = pair.getSecond();
			if (child != null) {
				return child.onInvoke(req, restOfPath);
			}
			throw new DSInvalidPathException(req.getPath());
		}
	}
	
	public void onSet(String path, DSElement value, DSPermission permission) {
		if (path.isEmpty() || path.equals("/")) {
			onSet(value, permission);
		} else {
			Pair<MyDSNode, String> pair = traverseDown(path);
			MyDSNode child = pair.getFirst();
			String restOfPath = pair.getSecond();
			if (child != null) {
				child.onSet(restOfPath, value, permission);
			}
		}
	}
	
	public void onSet(DSElement value, DSPermission permission) {
	}
	
	public OutboundListResponse onList(InboundListRequest req, String path) {
		if (path.isEmpty() || path.equals("/")) {
			final ApiObject me = this;
			this.listHandle = req;
			return new OutboundListResponse() {
				
				private ApiObject target = me;

				@Override
				public void onClose() {
					listHandle = null;
				}

				@Override
				public ApiObject getTarget() {
					return target;
				}
			};
		} else {
			Pair<MyDSNode, String> pair = traverseDown(path);
			MyDSNode child = pair.getFirst();
			String restOfPath = pair.getSecond();
			if (child == null) {
				throw new DSInvalidPathException(req.getPath());
			}
			return child.onList(req, restOfPath);
		}
	}
	
	public SubscriptionCloseHandler onSubscribe(InboundSubscribeRequest req, String path) {
		if (path.isEmpty() || path.equals("/")) {
			return subscribe(req);
		} else {
			Pair<MyDSNode, String> pair = traverseDown(path);
			MyDSNode child = pair.getFirst();
			String restOfPath = pair.getSecond();
			if (child == null) {
				return new SubscriptionCloseHandler() {
					@Override
					public void onClose(Integer subscriptionId) {
					}
				};
			}
			return child.onSubscribe(req, restOfPath);
		}
	}
	
	protected SubscriptionCloseHandler subscribe(InboundSubscribeRequest req) {
		return new SubscriptionCloseHandler() {
			@Override
			public void onClose(Integer subscriptionId) {
			}
		};
	}
	
	private Pair<MyDSNode, String> traverseDown(String path) {
		if (path.startsWith("/")) {
			path = path.substring(1);
		}
		String[] arr = path.split("/", 2);
		DSIObject obj = get(arr[0]);
		MyDSNode node = null;
		String restOfPath = arr.length > 1 ? arr[1] : "";
		if (obj instanceof MyDSNode) {
			node = (MyDSNode) obj;
		}
		return new Pair<MyDSNode, String>(node, restOfPath);
	}
	
//	@Override
//	public String getPathName() {
//		return DSPath.encodeName(getName());
//	}

	@Override
	public Iterator<ApiObject> getChildren() {
		List<ApiObject> childs = new ArrayList<ApiObject>();
		for (int i=0; i< childCount(); i++) {
			DSIObject obj = get(i);
			if (obj instanceof ApiObject) {
				childs.add((ApiObject) obj);
			}
		}
		return childs.iterator();
	}

	@Override
	public void getMetaData(DSMap metaData) {
		// TODO Auto-generated method stub
		
	}
	
	public void addChild(String name, MyDSNode child, boolean onStart) {
		put(name, child);
		setCachedName();
		if (!onStart) {
			child.start();
		}
		if (listHandle != null) {
			listHandle.childAdded(child);
		}
	}
	
	@Override
	public DSInfo remove(int idx) {
		DSInfo info = super.remove(idx);
		DSIObject child = info.getObject();
		if (listHandle != null && child instanceof ApiNode) {
			listHandle.childRemoved((ApiNode) child);
		}
		return info;
	}
	
	
	public void delete() {
		DSContainer parent = getParent();
		if (parent instanceof MyDSNode) {
			((MyDSNode) parent).remove(getName());
		} else {
			parent.remove(getName());
		}
	}
	
	private void setCachedName() {
		this.cachedName = super.getName();
	}
	
	@Override
	public String getName() {
		String name = super.getName();
		if (name == null) {
			return cachedName;
		}
		return name;
	}

}
