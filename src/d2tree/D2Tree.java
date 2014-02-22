package d2tree;

import java.lang.Thread.State;
import java.util.logging.Logger;

import p2p.simulator.message.Message;
import p2p.simulator.network.Network;
import p2p.simulator.protocol.Peer;

public class D2Tree extends Peer {

    private Network         Net;
    private D2TreeCore      Core;
    private D2TreeIndexCore indexCore;
    // private D2TreeRedistributionCore redistCore;
    private long            Id;
    private Thread.State    state;
    private boolean         isOnline;
    private int             introducer;

    // private long n;
    // private long k;

    @Override
    public void init(long id, long n, long k, Network Net) {

        this.Net = Net;
        this.Id = id;
        this.isOnline = false;
        this.state = Thread.State.NEW;
        this.Core = new D2TreeCore(Id, Net);
        this.indexCore = new D2TreeIndexCore(Id, Net);
        // this.redistCore = new D2TreeRedistributionCore(Id, Net);
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
            break;
        case D2TreeMessageT.JOIN_RES:
            Core.forwardJoinResponse(msg);
            isOnline = true;
            break;
        case D2TreeMessageT.CONNECT_MSG:
            Core.connect(msg);
            break;
        case D2TreeMessageT.REDISTRIBUTE_SETUP_REQ:
            Core.forwardBucketPreRedistributionRequest(msg);
            break;
        case D2TreeMessageT.REDISTRIBUTE_REQ:
            Core.forwardBucketRedistributionRequest(msg);
            break;
        case D2TreeMessageT.REDISTRIBUTE_RES:
            Core.forwardBucketRedistributionResponse(msg);
            break;
        case D2TreeMessageT.GET_SUBTREE_SIZE_REQ:
            Core.forwardGetSubtreeSizeRequest(msg);
            break;
        case D2TreeMessageT.GET_SUBTREE_SIZE_RES:
            Core.forwardGetSubtreeSizeResponse(msg);
            break;
        case D2TreeMessageT.CHECK_BALANCE_REQ:
            Core.forwardCheckBalanceRequest(msg);
            break;
        case D2TreeMessageT.EXTEND_CONTRACT_REQ:
            Core.forwardExtendContractRequest(msg);
            break;
        case D2TreeMessageT.EXTEND_REQ:
            Core.forwardExtendRequest(msg);
            break;
        case D2TreeMessageT.EXTEND_RES:
            Core.forwardExtendResponse(msg);
            break;
        case D2TreeMessageT.CONTRACT_REQ:
            Core.forwardContractRequest(msg);
            break;
        case D2TreeMessageT.TRANSFER_REQ:
            Core.forwardTransferRequest(msg);
            break;
        case D2TreeMessageT.TRANSFER_RES:
            Core.forwardTransferResponse(msg);
            break;
        case D2TreeMessageT.DISCONNECT_MSG:
            Core.disconnect(msg);
            break;
        case D2TreeMessageT.LOOKUP_REQ:
            indexCore.lookup(msg, Core.getRT());
            break;
        case D2TreeMessageT.LOOKUP_RES:
            indexCore.decreasePendingQueries();
            break;
        case D2TreeMessageT.DELETE_REQ:
            indexCore.lookup(msg, Core.getRT());
            break;
        case D2TreeMessageT.DELETE_RES:
            indexCore.decreasePendingQueries();
            break;
        case D2TreeMessageT.INSERT_REQ:
            indexCore.lookup(msg, Core.getRT());
            break;
        case D2TreeMessageT.INSERT_RES:
            indexCore.decreasePendingQueries();
            break;
        case D2TreeMessageT.PRINT_MSG:
            Core.printTree(msg);
            break;
        default:
            System.out.println("Unrecognized message type: " + mType);
        }
    }

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

        indexCore.increasePendingQueries();
        msg = new Message(Id, introducer, new LookupRequest(key));
        indexCore.lookup(msg, Core.getRT());
        // if (result == key) pendingQueries--;
        // else throw new IOException();
    }

    @Override
    public void insert(long key) {
        Message msg = new Message(Id, introducer, new InsertRequest(key));
        indexCore.lookup(msg, Core.getRT());
    }

    @Override
    public void delete(long key) {
        Message msg = new Message(Id, introducer, new DeleteRequest(key));
        indexCore.lookup(msg, Core.getRT());
    }

    @Override
    public void registerLogger(Logger logger) {}

    @Override
    public int getRTSize() {
        return Core.getRtSize();
    }

    @Override
    public int getPendingQueries() {
        return indexCore.getPendingQueries();
    }

}
