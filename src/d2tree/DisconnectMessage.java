package d2tree;

import p2p.simulator.message.MessageBody;
import d2tree.RoutingTable.Role;

public class DisconnectMessage extends MessageBody {

    private static final long serialVersionUID = 3768244034950885149L;

    private Role              role;
    private int               index;
    private long              nodeToRemove;
    private long              initialNode;

    public DisconnectMessage(long nodeToRemove, Role role, long initialNode) {
        this.role = role;
        this.nodeToRemove = nodeToRemove;
        this.initialNode = initialNode;
    }

    public DisconnectMessage(long nodeToRemove, Role role, int index,
            long initialNode) {
        this.role = role;
        this.nodeToRemove = nodeToRemove;
        this.index = index;
        this.initialNode = initialNode;
    }

    long getInitialNode() {
        return initialNode;
    }

    public Role getRole() {
        return role;
    }

    public int getIndex() {
        return index;
    }

    public long getNodeToRemove() {
        return this.nodeToRemove;
    }

    @Override
    public int getType() {
        // TODO Auto-generated method stub
        return D2TreeMessageT.DISCONNECT_MSG;
    }

}
