package org.iot.dsa.servicebus.node;

import org.iot.dsa.dslink.DSPermissionException;
import org.iot.dsa.dslink.responder.ApiValue;
import org.iot.dsa.dslink.responder.InboundSubscribeRequest;
import org.iot.dsa.dslink.responder.SubscriptionCloseHandler;
import org.iot.dsa.node.DSElement;
import org.iot.dsa.security.DSPermission;

public class MyDSValueNode extends MyDSNode implements ApiValue {
	
	private DSElement value = null;
	InboundSubscribeRequest subscriptionHandle = null;
	private DSPermission writable = null;
	private Handler<ValuePair> handler = null;
	

	public void setWritePermission(DSPermission writable) {
		if (writable == null || writable.getLevel() < DSPermission.WRITE.getLevel()) {
			this.writable = null;
		} else {
			this.writable = writable;
		}
	}
	
	public void setValue(DSElement value) {
		this.value = value;
		if (subscriptionHandle != null && value != null) {
			try {
				subscriptionHandle.update(System.currentTimeMillis(), value, 0);
			} catch (Exception e) {
				
			}
		}
	}
	
	@Override
	public DSElement getValue() {
		return value;
	}
	
	@Override
	protected SubscriptionCloseHandler subscribe(InboundSubscribeRequest req) {
		subscriptionHandle = req;
		if (value != null) {
			req.update(System.currentTimeMillis(), value, 0);
		}
        return new SubscriptionCloseHandler() {
            @Override
            public void onClose(Integer subscriptionId) {
            	subscriptionHandle = null;
            }
        };
	}
	
	@Override
	public void onSet(DSElement value, DSPermission permission) {
		if (writable != null && permission.getLevel() >= writable.getLevel()) {
			DSElement old = this.value;
			setValue(value);
			if (handler != null) {
				handler.handle(new ValuePair(old, value));
			}
		} else {
			throw new DSPermissionException("");
		}
	}
	
	public void setOnSetHandler(Handler<ValuePair> handler) {
		this.handler = handler;
	}

//	@Override
//	public void getMetaData(DSMap metaData) {
//		if (writable != null) {
//			metaData.put("writable", writable.toString());
//		} else {
//			metaData.remove("writable");
//		}
//	}

	@Override
	public DSPermission getWritePermission() {
		return writable;
	}

}
