package d2tree;

import p2p.simulator.message.MessageT;

public class D2TreeMessageT extends MessageT {
    private static final long serialVersionUID       = 2160112452787414344L;
    static final int          JOIN_REQ               = 1001;
    static final int          JOIN_RES               = 1002;
    static final int          LEAVE_REQ             = 1003;
    static final int          LEAVE_RES             = 1004;

    static final int          CONNECT_MSG            = 1011;
    static final int          DISCONNECT_MSG         = 1012;

    static final int          REDISTRIBUTE_REQ       = 1021;
    static final int          REDISTRIBUTE_RES       = 1022;
    static final int          REDISTRIBUTE_SETUP_REQ = 1023;

    static final int          GET_SUBTREE_SIZE_REQ   = 1031;
    static final int          GET_SUBTREE_SIZE_RES   = 1032;

    static final int          CHECK_BALANCE_REQ      = 1041;

    static final int          EXTEND_CONTRACT_REQ    = 1051;
    static final int          EXTEND_REQ             = 1052;
    static final int          EXTEND_RES             = 1053;
    static final int          CONTRACT_REQ           = 1054;

    static final int          TRANSFER_REQ           = 1061;
    static final int          TRANSFER_RES           = 1062;

    static final int          PRINT_MSG              = 1071;
    static final int          PRINT_ERR_MSG          = 1072;

    // index
    static final int          VWUPDATE_REQ           = 2001;
    static final int          VWUPDATE_RES           = 2002;

    static final int          REPLACE_KEY_REQ        = 2011;
    static final int          REPLACE_KEY_RES        = 2012;

    // static final int LOOKUP_REQ = 1019;
    // static final int LOOKUP_RES = 1020;
    // static final int INSERT_REQ = 1021;
    // static final int INSERT_RES = 1022;
    // static final int DELETE_REQ = 1023;
    // static final int DELETE_RES = 1024;

    public static String toString(int msgType) {
        switch (msgType) {
        case JOIN_REQ:
            return "JOIN REQUEST";
        case JOIN_RES:
            return "JOIN RESPONSE";
        case LEAVE_REQ:
            return "DEPART REQUEST";
        case LEAVE_RES:
            return "DEPART RESPONSE";

        case CONNECT_MSG:
            return "CONNECT MESSAGE";
        case DISCONNECT_MSG:
            return "DISCONNECT REQUEST";

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

        case PRINT_MSG:
            return "PRINT MESSAGE";
        case PRINT_ERR_MSG:
            return "PRINT ERROR MESSAGE";

        case VWUPDATE_REQ:
            return "V.WEIGHT UPDATE REQUEST";
        case VWUPDATE_RES:
            return "V.WEIGHT UPDATE RESPONSE";

        case REPLACE_KEY_REQ:
            return "KEY REPLACEMENT REQUEST";
        case REPLACE_KEY_RES:
            return "KEY REPLACEMENT RESPONSE";

        default:
            return "WRONG TYPE";
        }
    }
}
