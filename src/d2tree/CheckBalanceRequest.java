package d2tree;

import p2p.simulator.message.MessageBody;

public class CheckBalanceRequest extends MessageBody {

	private static final long serialVersionUID = 2100066121599892428L;
	private long initialNode;

	public CheckBalanceRequest(long initialNode) {
		this.initialNode = initialNode;
    }
	long getInitialNode(){
		return initialNode;
	}
    
    @Override
    public int getType() {
        return D2TreeMessageT.CHECK_BALANCE_REQ;
    }

}
