package d2tree;

import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.logging.Logger;

import p2p.simulator.message.Message;
import p2p.simulator.network.Network;
import p2p.simulator.protocol.Peer;
import d2tree.RoutingTable.Role;

public class D2Tree extends Peer {

    private Random                       randomGenerator;
    private Network                      Net;
    private D2TreeCore                   Core;
    private D2TreeIndexCore              indexCore;
    // private D2TreeRedistributionCore redistCore;
    private long                         Id;
    private Thread.State                 state;
    private boolean                      isOnline;
    // private int introducer;
    private static ArrayList<Integer>    introducers;
    private static HashMap<Long, D2Tree> allNodes;

    private int getRandomIntroducer() {
        randomGenerator = new Random();
        int index = randomGenerator.nextInt(D2Tree.introducers.size());
        return D2Tree.introducers.get(index);
    }

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
        // this.introducer = 1;
        if (D2Tree.introducers == null)
            D2Tree.introducers = new ArrayList<Integer>();
        if (D2Tree.introducers.isEmpty()) D2Tree.introducers.add(1);

        if (id == 1) isOnline = true;

        if (allNodes == null) allNodes = new HashMap<Long, D2Tree>();
        allNodes.put(Id, this);
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
            if (Core.isLeaf()) {
                long lastBucketNode = Core.getRT().get(Role.LAST_BUCKET_NODE);
                long rightAdjNode = Core.getRT().get(Role.RIGHT_A_NODE);
                double value = D2Tree.generateRandomHandyValue(lastBucketNode,
                        rightAdjNode);
                indexCore.addValue(value);
            }
            Core.forwardJoinRequest(msg);
            break;
        case D2TreeMessageT.JOIN_RES:
            Core.forwardJoinResponse(msg);
            isOnline = true;
            break;
        case D2TreeMessageT.DEPART_REQ:
            this.forwardLeaveRequest(msg);
            break;
        // case D2TreeMessageT.DEPART_RES:
        // Core.forwardLeaveResponse(msg);
        // isOnline = false;
        // break;
        case D2TreeMessageT.CONNECT_MSG:
            ConnectMessage connMsg = (ConnectMessage) msg.getData();
            if (connMsg.getRole() == Role.RIGHT_A_NODE) Core.connect(msg);
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

        // introducers.add((int) this.Id);
        // msg = new Message(Id, introducer, new JoinRequest());
        msg = new Message(Id, getRandomIntroducer(), new JoinRequest());
        Net.sendMsg(msg);
    }

    @Override
    public void leavePeer() {

        // find replacement node

        // migrate data

        // switch links

        Message msg = new Message(Id, Id, new LeaveRequest(Id));
        this.forwardLeaveRequest(msg);

        // Net.sendMsg(msg);
    }

    @Override
    public void lookup(long key) {

        Message msg;

        indexCore.increasePendingQueries();
        // msg = new Message(Id, introducer, new LookupRequest(key));
        msg = new Message(Id, getRandomIntroducer(), new LookupRequest(key));
        indexCore.lookup(msg, Core.getRT());
        // if (result == key) pendingQueries--;
        // else throw new IOException();
    }

    @Override
    public void insert(long key) {
        // Message msg = new Message(Id, introducer, new InsertRequest(key));
        Message msg = new Message(Id, getRandomIntroducer(), new InsertRequest(
                key));
        indexCore.lookup(msg, Core.getRT());
    }

    @Override
    public void delete(long key) {
        // Message msg = new Message(Id, introducer, new DeleteRequest(key));
        Message msg = new Message(Id, getRandomIntroducer(), new DeleteRequest(
                key));
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

    void forwardLeaveRequest(Message msg) {
        assert msg.getData() instanceof LeaveRequest;
        LeaveRequest data = (LeaveRequest) msg.getData();

        // Step 1: find out the id of the next node
        Role previousRole = data.getNewRole();
        Role nextRole = LeaveRequest.getNextRole(Core);
        long nextNode = Core.getRT().get(nextRole);

        if (nextNode == RoutingTable.DEF_VAL) { // we've reached the bucket

        }

        // Step 2: collect all data
        D2TreeCore forwardCore = new D2TreeCore(Core);
        D2TreeIndexCore forwardIndexCore = new D2TreeIndexCore(indexCore);

        if (Id == data.getLeaveNodeID()) { // if this node is the one to leave,
                                           // remove its entry from the nodelist
            D2TreeCore.routingTables.remove(Id);
        }

        // Step 3: Send data to the next node
        data = new LeaveRequest(data.getLeaveNodeID(), forwardCore,
                forwardIndexCore);
        data.setNewRole(nextRole);
        data.setOldRole(previousRole);
        Net.sendMsg(new Message(Id, nextNode, data));

        // Step 4: Replace node content with the one sent by the preceding node

        if (data.getLeaveNodeID() == Id) {
            // do nothing
        }
        else {
            D2TreeCore newCore = data.getCore();
            this.Core = new D2TreeCore(newCore);
            this.Core.fixBrokenLinks(Id, nextNode);

            D2TreeIndexCore newIndexCore = data.getIndexCore();
            this.indexCore = new D2TreeIndexCore(newIndexCore);
            this.indexCore.setID(Id);
        }
    }

    static double generateRandomHandyValue(long leftAdjNodeID,
            long rightAdjNodeID) {
        D2Tree leftAdjNode = allNodes.get(leftAdjNodeID);
        double minValue = Collections.min(leftAdjNode.indexCore.keys);

        D2Tree rightAdjNode = allNodes.get(rightAdjNodeID);
        double maxValue = Collections.max(rightAdjNode.indexCore.keys);

        assert maxValue > minValue;
        double diff = maxValue - minValue;
        return Math.random() * diff + minValue;
    }
}
