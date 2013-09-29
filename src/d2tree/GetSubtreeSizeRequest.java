package d2tree;

import d2tree.D2TreeCore.Mode;
import p2p.simulator.message.MessageBody;

public class GetSubtreeSizeRequest extends MessageBody {
	private static final long serialVersionUID = -5767816851951973009L;
	private Mode mode;
	private long size;
	private long initialNode;
	
	public GetSubtreeSizeRequest(Mode mode, long initialNode){
		this.mode = mode;
		this.size = 0;
		this.initialNode = initialNode;
	}
	long getInitialNode(){
		return initialNode;
	}
	public Mode getMode(){
		return this.mode;
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
