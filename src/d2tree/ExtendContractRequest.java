package d2tree;

import p2p.simulator.message.MessageBody;

public class ExtendContractRequest extends MessageBody {
	
	private static final long serialVersionUID = -671508466340485564L;
	private int msgType;
	
    public ExtendContractRequest(int msgType) {
    	this.msgType = msgType;
    }
    
    @Override
    public int getType() {
        return this.msgType;
    }

}
