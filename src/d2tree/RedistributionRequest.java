package d2tree;

import p2p.simulator.message.MessageBody;

public class RedistributionRequest extends MessageBody {
	
	private static final long serialVersionUID = 3137280817627914419L;
	static long DEF_VAL = -1;
	private long subtreeID; //the id of this subtree's root
	private long noofUncheckedBucketNodes;
	private long noofUncheckedBuckets;
	private long transferDest;
	private long initialNode;
	
    public RedistributionRequest(long noofUncheckedBucketNodes, long noofUncheckedBuckets, long subtreeID, long initialNode) {
    	this.noofUncheckedBucketNodes = noofUncheckedBucketNodes;
    	this.noofUncheckedBuckets = noofUncheckedBuckets;
    	this.subtreeID = subtreeID;
    	transferDest = DEF_VAL;
    	this.initialNode = initialNode;
    }
    public long getNoofUncheckedBucketNodes(){
    	return noofUncheckedBucketNodes;
    }
    public long getNoofUncheckedBuckets(){
    	return noofUncheckedBuckets;
    	
    }
    long getSubtreeID(){
    	return subtreeID;
    }
	long getInitialNode(){
		return initialNode;
	}
    long getTransferDest(){
    	return transferDest;
    }
    void setTransferDest(long dest){
    	this.transferDest = dest;
    }
    
    @Override
    public int getType() {
        return D2TreeMessageT.REDISTRIBUTE_REQ;
    }

}
