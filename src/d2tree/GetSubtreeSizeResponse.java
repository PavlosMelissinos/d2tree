package d2tree;

import p2p.simulator.message.MessageBody;

public class GetSubtreeSizeResponse extends MessageBody {
	private static final long serialVersionUID = -5767816851951973009L;
	private long size;
	private long finalDestID;
	private long initialNode;
	
//	public GetSubtreeSizeResponse(long initialNode){
//		this.size = 0;
//		this.initialNode = initialNode;
//	}
	public GetSubtreeSizeResponse(long size, long finalDestID, long initialNode){
		this.size = size;
		this.finalDestID = finalDestID;
		this.initialNode = initialNode;
	}
	long getInitialNode(){
		return initialNode;
	}
	public long getDestinationID(){
		return this.finalDestID;
	}
	public long getSize(){
		return this.size;
	}
	/*public void incrementSize(){
		this.size++;
	}*/
	@Override
	public int getType() {
        return D2TreeMessageT.GET_SUBTREE_SIZE_RES;
	}

}
