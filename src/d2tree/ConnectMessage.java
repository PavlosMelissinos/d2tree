package d2tree;

import d2tree.RoutingTable.Role;
import p2p.simulator.message.MessageBody;

public class ConnectMessage extends MessageBody {
	private static final long serialVersionUID = 1363832437446636196L;

	private Role role;
	private long node;
	private int index;
	public ConnectMessage(long node, Role role){
		this.role = role;
		this.index = 0;
	}
	public ConnectMessage(long node, Role role, int index){
		this.role = role;
		this.index = index;
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
