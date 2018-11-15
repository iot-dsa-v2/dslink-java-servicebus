package org.iot.dsa.servicebus;

import org.iot.dsa.node.DSInfo;
import org.iot.dsa.node.DSNode;
import org.iot.dsa.node.action.ActionInvocation;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.node.action.DSAction;

/**
 * The base class for any node that has a "remove" action.
 *
 * @author Daniel Shapiro
 */
public class RemovableNode extends DSNode {

    @Override
    protected void declareDefaults() {
        declareDefault("Remove", makeRemoveAction());
    }

    protected DSAction makeRemoveAction() {
        return new DSAction.Parameterless() {
            @Override
            public ActionResult invoke(DSInfo target, ActionInvocation invocation) {
                ((RemovableNode) target.get()).delete();
                return null;
            }
        };
    }

    public void delete() {
        getParent().remove(getName());
    }

}
