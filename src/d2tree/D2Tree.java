package d2tree;

import java.lang.Thread.State;
import java.util.logging.Logger;

import p2p.simulator.message.Message;
import p2p.simulator.network.Network;
import p2p.simulator.protocol.Peer;

public class D2Tree extends Peer {

    private Network      Net;
    private D2TreeCore   Core;
    private long         Id;
    private Thread.State state;
    private boolean      isOnline;
    private int          pendingQueries;
    private int          introducer;

    @Override
    public void init(long id, long n, long k, Network Net) {

        this.Net = Net;
        this.Id = id;
        this.isOnline = false;
        this.state = Thread.State.NEW;
        this.Core = new D2TreeCore(Id, Net);
        this.pendingQueries = 0;
        this.introducer = 1;

        if (id == 1) isOnline = true;
    }

    @Override
    public void run() {

        Message msg;

        if ((msg = Net.recvMsg(Id)) != null) resolveMessage(msg);

        this.state = Thread.State.TERMINATED;
    }

    private void resolveMessage(Message msg) {

        int mType;
        PrintMessage.print(msg, "Node " + msg.getDestinationId() +
                " received message from " + msg.getSourceId(),
                PrintMessage.logDir + "messages.txt");
        mType = msg.getType();

        // HashMap<Role, Integer> oldInconsistencies = Core
        // .findRTInconsistencies();
        // boolean isInconsistent = false;
        // Core.findRTInconsistencies();
        switch (mType) {
        case D2TreeMessageT.JOIN_REQ:
            Core.forwardJoinRequest(msg);
            // postActionCheck(oldInconsistencies, mType);
            break;
        case D2TreeMessageT.JOIN_RES:
            Core.forwardJoinResponse(msg);
            // postActionCheck(oldInconsistencies, mType);
            isOnline = true;
            break;
        case D2TreeMessageT.CONNECT_MSG:
            Core.connect(msg);
            // postActionCheck(oldInconsistencies, mType);
            // isOnline = true;
            break;
        case D2TreeMessageT.REDISTRIBUTE_REQ:
            Core.forwardBucketRedistributionRequest(msg);
            // postActionCheck(oldInconsistencies, mType);
            break;
        case D2TreeMessageT.REDISTRIBUTE_RES:
            Core.forwardBucketRedistributionResponse(msg);
            // postActionCheck(oldInconsistencies, mType);
            break;
        case D2TreeMessageT.GET_SUBTREE_SIZE_REQ:
            Core.forwardGetSubtreeSizeRequest(msg);
            // postActionCheck(oldInconsistencies, mType);
            break;
        case D2TreeMessageT.GET_SUBTREE_SIZE_RES:
            Core.forwardGetSubtreeSizeResponse(msg);
            // postActionCheck(oldInconsistencies, mType);
            break;
        case D2TreeMessageT.CHECK_BALANCE_REQ:
            Core.forwardCheckBalanceRequest(msg);
            // postActionCheck(oldInconsistencies, mType);
            break;
        case D2TreeMessageT.EXTEND_CONTRACT_REQ:
            Core.forwardExtendContractRequest(msg);
            // postActionCheck(oldInconsistencies, mType);
            break;
        case D2TreeMessageT.EXTEND_REQ:
            Core.forwardExtendRequest(msg);
            // postActionCheck(oldInconsistencies, mType);
            break;
        case D2TreeMessageT.EXTEND_RES:
            Core.forwardExtendResponse(msg);
            // postActionCheck(oldInconsistencies, mType);
            break;
        case D2TreeMessageT.CONTRACT_REQ:
            Core.forwardContractRequest(msg);
            // postActionCheck(oldInconsistencies, mType);
            break;
        case D2TreeMessageT.TRANSFER_REQ:
            Core.forwardTransferRequest(msg);
            // postActionCheck(oldInconsistencies, mType);
            break;
        case D2TreeMessageT.TRANSFER_RES:
            Core.forwardTransferResponse(msg);
            // postActionCheck(oldInconsistencies, mType);
            break;
        case D2TreeMessageT.DISCONNECT_MSG:
            Core.disconnect(msg);
            // postActionCheck(oldInconsistencies, mType);
            break;
        case D2TreeMessageT.LOOKUP_REQ:
            Core.forwardLookupRequest(msg);
            // postActionCheck(oldInconsistencies, mType);
            break;
        case D2TreeMessageT.LOOKUP_RES:
            pendingQueries--;
            // postActionCheck(oldInconsistencies, mType);
            break;
        case D2TreeMessageT.PRINT_MSG:
            D2TreeCore.printTree(msg);
            break;
        default:
            System.out.println("Unrecognized message type: " + mType);
        }
        // if (wasConsistent && isInconsistent) {
        // // IllegalStateException e = new IllegalStateException();
        // new Exception().printStackTrace();
        // }
    }

    // void postActionCheck(HashMap<Role, Integer> oldInconsistencies, int
    // msgType) {
    // Core.findRTInconsistencies(true, oldInconsistencies, msgType);
    // Core.findFixedRTInconsistencies(oldInconsistencies);
    // }

    @Override
    public long getPeerId() {
        return this.Id;
    }

    @Override
    public int getNumOfKeys() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isOnline() {
        return this.isOnline;
    }

    @Override
    public void setState(State state) {
        this.state = state;
    }

    @Override
    public State getState() {
        return this.state;
    }

    @Override
    public void joinPeer() {

        Message msg;

        msg = new Message(Id, introducer, new JoinRequest());
        Net.sendMsg(msg);
    }

    @Override
    public void leavePeer() {
        // throw new UnsupportedOperationException("Not supported yet.");
        // System.out.println("Not supported yet.");
    }

    @Override
    public void lookup(long key) {

        Message msg;

        pendingQueries++;
        msg = new Message(Id, introducer, new LookupRequest(key));
        Core.forwardLookupRequest(msg);
        // pendingQueries--;
    }

    @Override
    public void insert(long key) {
        // throw new UnsupportedOperationException("Not supported yet.");
        // System.out.println("Not supported yet.");
    }

    @Override
    public void delete(long key) {
        // throw new UnsupportedOperationException("Not supported yet.");
        // System.out.println("Not supported yet.");
    }

    @Override
    public void registerLogger(Logger logger) {}

    @Override
    public int getRTSize() {
        return Core.getRtSize();
    }

    @Override
    public int getPendingQueries() {
        return this.pendingQueries;
    }

}
