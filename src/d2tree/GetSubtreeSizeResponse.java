package d2tree;

import p2p.simulator.message.MessageBody;

public class GetSubtreeSizeResponse extends MessageBody {
	private static final long serialVersionUID = -5767816851951973009L;
	private long size;
	private long finalDestID;
	
	public GetSubtreeSizeResponse(){
		this.size = 1;
	}	
	public GetSubtreeSizeResponse(long size, long finalDestID){
		this.size = size;
		this.finalDestID = finalDestID;
	}
	public long getDestinationID(){
		return this.finalDestID;
	}
	public long getSize(){
		return this.size;
	}
	public void incrementSize(){
		this.size++;
	}
	@Override
	public int getType() {
        return D2TreeMessageT.GET_SUBTREE_SIZE_REQ;
	}

}
