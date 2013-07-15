package d2tree;

import p2p.simulator.message.MessageBody;

public class TransferResponse extends MessageBody {
	
	public enum TransferType{
		NODE_REMOVED,
		NODE_ADDED
	}
	
	private TransferType tType;
	private long pivotBucket;
	public TransferResponse(TransferType tType, long pivotBucket){
		this.tType = tType;
		this.pivotBucket = pivotBucket;
	}
	public TransferType getTransferType(){
		return this.tType;
	}
	public long getPivotBucket(){
		return this.pivotBucket;
	}
	@Override
	public int getType() {
		// TODO Auto-generated method stub
		return D2TreeMessageT.TRANSFER_RES;
	}
}
