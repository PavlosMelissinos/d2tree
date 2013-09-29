package d2tree;

import d2tree.D2TreeCore.Mode;
import p2p.simulator.message.MessageBody;

public class GetSubtreeSizeResponse extends MessageBody {
	private static final long serialVersionUID = -5767816851951973009L;
	private long size;
	private long finalDestID;
	private long initialNode;
	private Mode mode;
	
//	public GetSubtreeSizeResponse(long initialNode){
//		this.size = 0;
//		this.initialNode = initialNode;
//	}
	public GetSubtreeSizeResponse(Mode mode, long size, long finalDestID, long initialNode){
		this.mode = mode;
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
	public Mode getMode(){
		return this.mode;
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
