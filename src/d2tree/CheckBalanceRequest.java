package d2tree;

import p2p.simulator.message.MessageBody;

public class CheckBalanceRequest extends MessageBody {

	private static final long serialVersionUID = 2100066121599892428L;
	private long initialNode;
	private long totalBucketSize;

	public CheckBalanceRequest(long totalBucketSize, long initialNode) {
		this.totalBucketSize = totalBucketSize;
		this.initialNode = initialNode;
    }
	long getInitialNode(){
		return initialNode;
	}
    
	long getTotalBucketSize(){
		return totalBucketSize;
	}
	
    @Override
    public int getType() {
        return D2TreeMessageT.CHECK_BALANCE_REQ;
    }

}
