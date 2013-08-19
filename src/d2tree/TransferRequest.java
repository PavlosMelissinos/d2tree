package d2tree;

import p2p.simulator.message.MessageBody;

public class TransferRequest extends MessageBody {

	private static final long serialVersionUID = 6271863293693936428L;
	private boolean firstPass;
	private long destBucket;
	private long pivotBucket;
	private long initialNode;
	
	public TransferRequest(long destBucket, long pivotBucket, boolean firstPass, long initialNode){
		this.destBucket = destBucket;
		this.firstPass = firstPass;
		this.initialNode = initialNode;
	}
	public long getDestBucket(){
		return this.destBucket;
	}
	public long getPivotBucket(){
		return this.pivotBucket;
	}
	public boolean isFirstPass(){
		return this.firstPass;
	}
	public long getInitialNode(){
		return this.initialNode;
	}
	@Override
	public int getType() {
		// TODO Auto-generated method stub
		return D2TreeMessageT.TRANSFER_REQ;
	}

}
