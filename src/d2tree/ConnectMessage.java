package d2tree;

import p2p.simulator.message.MessageBody;
import d2tree.RoutingTable.Role;

public class ConnectMessage extends MessageBody {
    private static final long serialVersionUID = 1363832437446636196L;

    private Role              role;
    private long              node;
    private int               index;
    private long              initialNode;
    private boolean           replace;

    public ConnectMessage(long node, Role role, boolean replace,
            long initialNode) {
        // assert node != RoutingTable.DEF_VAL;
        this.role = role;
        this.node = node;
        this.index = 0;
        this.replace = replace;
        this.initialNode = initialNode;
    }

    public ConnectMessage(long node, Role role, int index, boolean replace,
            long initialNode) {
        // assert node != RoutingTable.DEF_VAL;
        this.role = role;
        this.node = node;
        this.index = index;
        this.replace = replace;
        this.initialNode = initialNode;
    }

    public long getInitialNode() {
        return initialNode;
    }

    public Role getRole() {
        return this.role;
    }

    public long getNode() {
        return this.node;
    }

    public int getIndex() {
        return this.index;
    }

    public boolean replaces() {
        return this.replace;
    }

    @Override
    public int getType() {
        return D2TreeMessageT.CONNECT_MSG;
    }

}
