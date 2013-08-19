package d2tree;

import d2tree.RoutingTable.Role;
import p2p.simulator.message.MessageBody;

public class ConnectMessage extends MessageBody {
	private static final long serialVersionUID = 1363832437446636196L;

	private Role role;
	private long node;
	private int index;
	private long initialNode;
	public ConnectMessage(long node, Role role, long initialNode){
		this.role = role;
		this.node = node;
		this.index = 0;
		this.initialNode = initialNode;
	}
	public ConnectMessage(long node, Role role, int index, long initialNode){
		this.role = role;
		this.node = node;
		this.index = index;
		this.initialNode = initialNode;
	}
	long getInitialNode(){
		return initialNode;
	}
	public Role getRole(){
		return this.role;
	}
    public long getNode(){
    	return this.node;
    }
    public int getIndex(){
    	return this.index;
    }
    @Override
    public int getType() {
        return D2TreeMessageT.JOIN_RES;
    }

}
