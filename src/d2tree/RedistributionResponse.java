package d2tree;

import p2p.simulator.message.MessageBody;

public class RedistributionResponse extends MessageBody {

	private long destSize;
	public RedistributionResponse(long destSize) {
		this.destSize = destSize;
	}
	public long getDestSize(){
		return this.destSize;
	}
	@Override
	public int getType() {
		// TODO Auto-generated method stub
		return D2TreeMessageT.REDISTRIBUTE_RES;
	}

}
