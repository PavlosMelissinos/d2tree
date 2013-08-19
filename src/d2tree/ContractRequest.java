package d2tree;

import p2p.simulator.message.MessageBody;

public class ContractRequest extends MessageBody {
	
	private static final long serialVersionUID = -671508466340485564L;

	private long optimalBucketSize;
	private long initialNode;
	
    public ContractRequest(long optimalBucketSize, long initialNode) {
    	this.optimalBucketSize = optimalBucketSize;
    	this.initialNode = initialNode;
    }

	long getInitialNode(){
		return initialNode;
	}
    public long getOptimalBucketSize(){
    	return this.optimalBucketSize;
    }
    
    @Override
    public int getType() {
        return D2TreeMessageT.CONTRACT_REQ;
    }

}
