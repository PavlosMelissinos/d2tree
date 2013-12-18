package d2tree;

import p2p.simulator.message.MessageT;

public class D2TreeMessageT extends MessageT {
    private static final long serialVersionUID       = 2160112452787414344L;
    static final int          JOIN_REQ               = 1001;
    static final int          JOIN_RES               = 1002;
    static final int          CONNECT_MSG            = 1003;
    static final int          REDISTRIBUTE_REQ       = 1004;
    static final int          REDISTRIBUTE_RES       = 1005;
    static final int          GET_SUBTREE_SIZE_REQ   = 1006;
    static final int          GET_SUBTREE_SIZE_RES   = 1007;
    static final int          CHECK_BALANCE_REQ      = 1008;
    static final int          EXTEND_CONTRACT_REQ    = 1009;
    static final int          EXTEND_REQ             = 1010;
    static final int          EXTEND_RES             = 1011;
    static final int          CONTRACT_REQ           = 1012;
    static final int          TRANSFER_REQ           = 1013;
    static final int          TRANSFER_RES           = 1014;
    static final int          DISCONNECT_MSG         = 1015;
    static final int          PRINT_MSG              = 1016;
    static final int          PRINT_ERR_MSG          = 1017;
    static final int          REDISTRIBUTE_SETUP_REQ = 1018;

    public static String toString(int msgType) {
        switch (msgType) {
        case JOIN_REQ:
            return "JOIN REQUEST";
        case JOIN_RES:
            return "JOIN RESPONSE";
        case CONNECT_MSG:
            return "CONNECT MESSAGE";
        case REDISTRIBUTE_REQ:
            return "REDISTRIBUTE REQUEST";
        case REDISTRIBUTE_RES:
            return "REDISTRIBUTE RESPONSE";
        case REDISTRIBUTE_SETUP_REQ:
            return "REDISTRIBUTE SETUP REQUEST";
        case GET_SUBTREE_SIZE_REQ:
            return "GET SUBTREE SIZE REQUEST";
        case GET_SUBTREE_SIZE_RES:
            return "GET SUBTREE SIZE RESPONSE";
        case CHECK_BALANCE_REQ:
            return "CHECK BALANCE REQUEST";
        case EXTEND_CONTRACT_REQ:
            return "EXTEND-CONTRACT REQUEST";
        case EXTEND_REQ:
            return "EXTEND REQUEST";
        case EXTEND_RES:
            return "EXTEND RESPONSE";
        case CONTRACT_REQ:
            return "CONTRACT REQUEST";
        case TRANSFER_REQ:
            return "TRANSFER REQUEST";
        case TRANSFER_RES:
            return "TRANSFER RESPONSE";
        case DISCONNECT_MSG:
            return "DISCONNECT REQUEST";
        case PRINT_MSG:
            return "PRINT MESSAGE";
        case PRINT_ERR_MSG:
            return "PRINT ERROR MESSAGE";
        default:
            return "WRONG TYPE";
        }
    }
}
