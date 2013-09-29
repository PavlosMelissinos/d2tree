package d2tree;

import p2p.simulator.message.MessageBody;

public class TransferResponse extends MessageBody {
	private static final long serialVersionUID = 6862379902007511532L;
	
	public enum TransferType{
		NODE_REMOVED,
		NODE_ADDED
	}
	
	private TransferType tType;
	private long pivotBucket;
	private long initialNode;
	public TransferResponse(TransferType tType, long pivotBucket, long initialNode){
		this.tType = tType;
		this.pivotBucket = pivotBucket;
		this.initialNode = initialNode;
	}
	public TransferType getTransferType(){
		return this.tType;
	}
	public long getPivotBucket(){
		return this.pivotBucket;
	}
	public long getInitialNode(){
		return initialNode;
	}
	@Override
	public int getType() {
		// TODO Auto-generated method stub
		return D2TreeMessageT.TRANSFER_RES;
	}
}
