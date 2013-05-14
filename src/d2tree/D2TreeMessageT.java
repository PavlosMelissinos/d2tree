package d2tree;

import p2p.simulator.message.MessageT;

public class D2TreeMessageT extends MessageT {
	private static final long serialVersionUID = 2160112452787414344L;
	static final int JOIN_REQ             = 1001;
    static final int JOIN_RES             = 1002;
    static final int REDISTRIBUTE_REQ     = 1003;
    static final int GET_SUBTREE_SIZE_REQ = 1004;
    static final int CHECK_BALANCE_REQ    = 1005;
//    static final int UPDATE_SIZE_REQ  = 1004;
}
