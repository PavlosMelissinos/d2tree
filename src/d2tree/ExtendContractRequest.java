package d2tree;

import p2p.simulator.message.MessageBody;

public class ExtendContractRequest extends MessageBody {
	
	private static final long serialVersionUID = -671508466340485564L;
	private long height;
	private long initialNode;
	
    public ExtendContractRequest(long height, long initialNode) {
    	this.height = height;
    	this.initialNode = initialNode;
    }

	long getInitialNode(){
		return initialNode;
	}
    long getHeight(){
    	return this.height;
    }
    @Override
    public int getType() {
        return D2TreeMessageT.EXTEND_CONTRACT_REQ;
    }

}
