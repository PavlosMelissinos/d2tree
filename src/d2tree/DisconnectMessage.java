package d2tree;

import d2tree.RoutingTable.Role;
import p2p.simulator.message.MessageBody;

public class DisconnectMessage extends MessageBody {

	private static final long serialVersionUID = 3768244034950885149L;

	private Role role;
	private long nodeToRemove;
	private long initialNode;
	
	public DisconnectMessage(long nodeToRemove, Role role, long initialNode){
		this.role = role;
		this.nodeToRemove = nodeToRemove;
		this.initialNode = initialNode;
	}
	long getInitialNode(){
		return initialNode;
	}
	public Role getRole(){
		return this.role;
	}
	public long getNodeToRemove(){
		return this.nodeToRemove;
	}
	@Override
	public int getType() {
		// TODO Auto-generated method stub
		return D2TreeMessageT.DISCONNECT_MSG;
	}

}
