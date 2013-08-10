package d2tree;

import p2p.simulator.message.MessageBody;

public class CheckBalanceRequest extends MessageBody {

	private static final long serialVersionUID = 2100066121599892428L;

	public CheckBalanceRequest() {
    }
    
    @Override
    public int getType() {
        return D2TreeMessageT.CHECK_BALANCE_REQ;
    }

}
