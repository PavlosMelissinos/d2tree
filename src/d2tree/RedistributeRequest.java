package d2tree;

import p2p.simulator.message.MessageBody;

public class RedistributeRequest extends MessageBody {
	
	private static final long serialVersionUID = 3137280817627914419L;
	static long DEF_VAL = -1;
	private long subtreeID; //the id of this subtree's root
	private long noofUncheckedBucketNodes;
	private long noofUncheckedBuckets;

    public RedistributeRequest(long noofUncheckedBucketNodes, long noofUncheckedBuckets, long subtreeID) {
    	this.noofUncheckedBucketNodes = noofUncheckedBucketNodes;
    	this.noofUncheckedBuckets = noofUncheckedBuckets;
    	this.subtreeID = subtreeID;
    }
    public long getNoofUncheckedBucketNodes(){
    	return noofUncheckedBucketNodes;
    }
    public long getNoofUncheckedBuckets(){
    	return noofUncheckedBuckets;
    	
    }
    long getSubtreeID(){
    	return this.subtreeID;
    }
    
    @Override
    public int getType() {
        return D2TreeMessageT.REDISTRIBUTE_REQ;
    }

}
