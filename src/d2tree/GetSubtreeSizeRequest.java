package d2tree;

import p2p.simulator.message.MessageBody;

public class GetSubtreeSizeRequest extends MessageBody {
	private static final long serialVersionUID = -5767816851951973009L;
	private long size;
	private long initialNode;
	
	public GetSubtreeSizeRequest(long initialNode){
		this.size = 0;
		this.initialNode = initialNode;
	}
	long getInitialNode(){
		return initialNode;
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
