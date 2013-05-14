package d2tree;

import p2p.simulator.message.MessageBody;

public class ConnectMessage extends MessageBody {
	private static final long serialVersionUID = 1363832437446636196L;
	private RoutingTable rt;

    public ConnectMessage(RoutingTable rt) {
        this.rt = rt;
    }

    public RoutingTable getRoutingTable() {
        return this.rt;
    }
    
    @Override
    public int getType() {
        return D2TreeMessageT.JOIN_RES;
    }

}
