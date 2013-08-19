package d2tree;

import p2p.simulator.message.MessageBody;

public class ExtendRequest extends MessageBody {
	
	private static final long serialVersionUID = -671508466340485564L;
	
	private long optimalBucketSize;
	private long oldOptimalBucketSize;
	long initialNode;
	
    public ExtendRequest(long optimalBucketSize, long oldOptimalBucketSize, long initialNode) {
    	this.optimalBucketSize = optimalBucketSize;
    	this.oldOptimalBucketSize = oldOptimalBucketSize;
    	this.initialNode = initialNode;
    }

	long getInitialNode(){
		return initialNode;
	}
    public long getOptimalBucketSize(){
    	return this.optimalBucketSize;
    }
    public long getOldOptimalBucketSize(){
    	return this.oldOptimalBucketSize;
    }
    @Override
    public int getType() {
        return D2TreeMessageT.EXTEND_REQ;
    }

}
