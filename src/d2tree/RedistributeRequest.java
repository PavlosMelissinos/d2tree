package d2tree;

import p2p.simulator.message.MessageBody;

public class RedistributeRequest extends MessageBody {
	private long size;

    public RedistributeRequest(long size) {
    	this.size = size;
    }
    
    long getSize(){
    	return size;
    }
    
    @Override
    public int getType() {
        return D2TreeMessageT.REDISTRIBUTE_REQ;
    }

}
