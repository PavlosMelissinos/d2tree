package d2tree;

import p2p.simulator.message.MessageBody;

public class GetSubtreeSizeRequest extends MessageBody {
	private static final long serialVersionUID = -5767816851951973009L;
	private long size;
	
	public GetSubtreeSizeRequest(){
		this.size = 1;
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
