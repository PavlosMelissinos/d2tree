package d2tree;

import p2p.simulator.message.MessageBody;

public class CheckBalanceRequest extends MessageBody {

    public CheckBalanceRequest() {
    }
    
    @Override
    public int getType() {
        return D2TreeMessageT.CHECK_BALANCE_REQ;
    }

}
