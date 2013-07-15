package d2tree;

import p2p.simulator.message.MessageBody;

public class ExtendContractRequest extends MessageBody {
	
	private static final long serialVersionUID = -671508466340485564L;
	private long height;
	
    public ExtendContractRequest(long height) {
    	this.height = height;
    }
    
    long getHeight(){
    	return this.height;
    }
    @Override
    public int getType() {
        return D2TreeMessageT.EXTEND_CONTRACT_REQ;
    }

}
