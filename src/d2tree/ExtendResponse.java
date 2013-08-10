package d2tree;

import p2p.simulator.message.MessageBody;

public class ExtendResponse extends MessageBody {
	private static final long serialVersionUID = -737297975099005858L;

	private int index;
	private long leftChild;
	private long rightChild;
	public ExtendResponse(int index, long leftChild, long rightChild) {
		this.index = index;
		this.leftChild = leftChild;
		this.rightChild = rightChild;
    }
    
	public int getIndex(){
		return this.index;
	}
	
	public long getLeftChild(){
		return this.leftChild;
	}
	
	public long getRightChild(){
		return this.rightChild;
	}
	
    @Override
    public int getType() {
        return D2TreeMessageT.EXTEND_RES;
    }

}
