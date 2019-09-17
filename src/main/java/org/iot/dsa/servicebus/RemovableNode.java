package org.iot.dsa.servicebus;

import org.iot.dsa.dslink.ActionResults;
import org.iot.dsa.node.DSNode;
import org.iot.dsa.node.action.DSAction;
import org.iot.dsa.node.action.DSIActionRequest;

/**
 * The base class for any node that has a "remove" action.
 *
 * @author Daniel Shapiro
 */
public class RemovableNode extends DSNode {

    public void delete() {
        getParent().remove(getName());
    }

    @Override
    protected void declareDefaults() {
        declareDefault("Remove", makeRemoveAction());
    }

    protected DSAction makeRemoveAction() {
        return new DSAction() {
            @Override
            public ActionResults invoke(DSIActionRequest req) {
                ((RemovableNode) req.getTarget()).delete();
                return null;
            }
        };
    }

}
