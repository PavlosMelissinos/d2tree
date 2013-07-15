package d2tree;

import p2p.simulator.message.MessageBody;

public class TransferRequest extends MessageBody {

	private static final long serialVersionUID = 6271863293693936428L;
	private boolean firstPass;
	private long destBucket;
	private long pivotBucket;
	public TransferRequest(long destBucket, long pivotBucket, boolean firstPass){
		this.destBucket = destBucket;
		this.firstPass = firstPass;
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
	@Override
	public int getType() {
		// TODO Auto-generated method stub
		return D2TreeMessageT.TRANSFER_REQ;
	}

}
