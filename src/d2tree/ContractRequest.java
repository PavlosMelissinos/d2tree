package d2tree;

import p2p.simulator.message.MessageBody;

public class ContractRequest extends MessageBody {
	
	private static final long serialVersionUID = -671508466340485564L;

	private long optimalBucketSize;
	
    public ContractRequest(long optimalBucketSize) {
    	this.optimalBucketSize = optimalBucketSize;
    }
    
    public long getOptimalBucketSize(){
    	return this.optimalBucketSize;
    }
    
    @Override
    public int getType() {
        return D2TreeMessageT.CONTRACT_REQ;
    }

}
