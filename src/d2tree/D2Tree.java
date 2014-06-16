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
            Core.connect(msg);

            ConnectMessage connMsg = (ConnectMessage) msg.getData();
            long pNode = getPrecedingNode();
            long sNode = getSucceedingNode();
            long addedNode = connMsg.getNode();
            if (pNode == addedNode || sNode == addedNode) {
                if (needsSorting(pNode, sNode)) {
                    int keysetSize = indexCore.keys.size();
                    indexCore.keys.clear();
                    indexCore.keys = generateRandomHandyValues(pNode, sNode,
                            keysetSize);
                }
            }
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

    private static double getMinimumKeyBound(long precedingNodeID) {
        D2Tree precedingNode = allNodes.get(precedingNodeID);
        double minValue = -Double.MAX_VALUE;
        if (precedingNode != null)
            minValue = Collections.max(precedingNode.indexCore.keys);
        return minValue;
    }

    private static double getMaximumKeyBound(long succeedingNodeID) {
        D2Tree succeedingNode = allNodes.get(succeedingNodeID);
        double maxValue = Double.MAX_VALUE;
        if (succeedingNode != null)
            maxValue = Collections.min(succeedingNode.indexCore.keys);
        return maxValue;
    }

    static double generateRandomHandyValue(long precedingNodeID,
            long succeedingNodeID) {
        double minValue = getMinimumKeyBound(precedingNodeID);
        double maxValue = getMaximumKeyBound(succeedingNodeID);

        assert maxValue > minValue;
        double diff = maxValue - minValue;
        return Math.random() * diff + minValue;
    }

    static ArrayList<Double> generateRandomHandyValues(long precedingNodeID,
            long succeedingNodeID, int times) {
        double minValue = getMinimumKeyBound(precedingNodeID);
        double maxValue = getMaximumKeyBound(succeedingNodeID);

        assert maxValue > minValue;
        double diff = maxValue - minValue;
        ArrayList<Double> generatedNumbers = new ArrayList<Double>();
        for (int i = 0; i < times; i++)
            generatedNumbers.add(Math.random() * diff + minValue);
        return generatedNumbers;
    }

    long getPrecedingNode() {
        RoutingTable rt = Core.getRT();
        long pNodeID = RoutingTable.DEF_VAL;
        if (!Core.isLeaf() && !Core.isBucketNode()) {
            pNodeID = rt.get(Role.LEFT_A_NODE);
            D2Tree pNode = allNodes.get(pNodeID);
            RoutingTable pNodeRT = pNode.Core.getRT();
            if (pNodeRT.contains(Role.LAST_BUCKET_NODE)) {
                pNodeID = pNodeRT.get(Role.LAST_BUCKET_NODE);
            }
        }
        else if (Core.isLeaf()) {
            if (rt.contains(Role.LEFT_A_NODE))
                pNodeID = rt.get(Role.LEFT_A_NODE);
        }
        else if (Core.isBucketNode()) {
            if (rt.isEmpty(Role.LEFT_RT)) pNodeID = rt.get(Role.REPRESENTATIVE);
            else pNodeID = rt.get(Role.LEFT_RT, 0);
        }
        return pNodeID;
    }

    long getSucceedingNode() {
        RoutingTable rt = Core.getRT();
        long sNodeID = RoutingTable.DEF_VAL;
        if (!Core.isLeaf() && !Core.isBucketNode()) {
            sNodeID = rt.get(Role.RIGHT_A_NODE);
        }
        else if (Core.isLeaf()) {
            if (!rt.contains(Role.FIRST_BUCKET_NODE)) sNodeID = rt
                    .get(Role.RIGHT_A_NODE);
            else sNodeID = rt.get(Role.FIRST_BUCKET_NODE);
        }
        else if (Core.isBucketNode()) {
            if (!rt.isEmpty(Role.RIGHT_RT)) sNodeID = rt.get(Role.RIGHT_RT, 0);
            else {
                long representativeID = rt.get(Role.REPRESENTATIVE);
                D2Tree representative = allNodes.get(representativeID);
                RoutingTable representativeRT = representative.Core.getRT();
                sNodeID = representativeRT.get(Role.RIGHT_A_NODE);
            }
        }
        return sNodeID;
    }

    private boolean needsSorting(long pNodeID, long sNodeID) {
        double minAllowedValue = D2Tree.getMinimumKeyBound(pNodeID);
        double maxAllowedValue = D2Tree.getMaximumKeyBound(sNodeID);
        double minValue = Collections.min(indexCore.keys);
        double maxValue = Collections.max(indexCore.keys);
        if (minValue < minAllowedValue || maxValue > maxAllowedValue) return true;
        else return false;
    }
}
