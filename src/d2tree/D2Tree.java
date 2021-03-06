package d2tree;

import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import p2p.simulator.message.Message;
import p2p.simulator.network.Network;
import p2p.simulator.protocol.Peer;
import d2tree.KeyReplacementRequest.Mode;
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
    private Logger                       logger;
    // private int introducer;
    private static ArrayList<Integer>    introducers;
    private static HashMap<Long, D2Tree> allNodes;

    private static int                   minHeight = 0;

    // private static ArrayList<>

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

        if (D2Tree.introducers == null)
            D2Tree.introducers = new ArrayList<Integer>();
        if (D2Tree.introducers.isEmpty()) D2Tree.introducers.add(1);

        if (allNodes == null) allNodes = new HashMap<Long, D2Tree>();
        allNodes.put(Id, this);

        int minPBTNodes = (int) Math.pow(2, minHeight) - 1;
        int minLeaves = (int) Math.pow(2, minHeight - 1);
        if (id <= minPBTNodes) {
            // we initialize the first 63 nodes for the tree structure
            initializePBT();
            isOnline = true;
            return;
        }
        else if (id < minLeaves * minHeight + minPBTNodes) {
            // the rest 192 (32 * 6) get into the buckets of the 32 resulting
            // leaves, 6 in each bucket
            initializeBuckets();
            isOnline = true;
            return;
        }
        // this.redistCore = new D2TreeRedistributionCore(Id, Net);
        // this.introducer = 1;

        if (id == 1) isOnline = true;
    }

    @Override
    public void run() {

        Message msg;

        if ((msg = Net.recvMsg(Id)) != null) resolveMessage(msg);

        this.state = Thread.State.TERMINATED;
    }

    private void resolveMessage(Message msg) {
        logger.logp(Level.FINEST, this.getClass().getName(), "resolveMsg",
                msg.toString());

        int mType;
        PrintMessage.print(msg, "Node " + msg.getDestinationId() +
                " received message from " + msg.getSourceId(), "messages.txt");
        mType = msg.getType();

        switch (mType) {
        case D2TreeMessageT.JOIN_REQ:
            if (Core.isLeaf()) {
                RoutingTable coreRT = Core.getRT();
                long precedingNode = coreRT.contains(Role.LAST_BUCKET_NODE) ? coreRT
                        .get(Role.LAST_BUCKET_NODE) : Id;
                long succeedingNode = Core.getRT().get(Role.RIGHT_A_NODE);
                D2Tree newNode = allNodes.get(msg.getSourceId());
                // assert newNode.indexCore.keys.isEmpty();
                long value = D2Tree.generateRandomHandyValue(precedingNode,
                        succeedingNode);
                newNode.indexCore.keys.clear();
                newNode.indexCore.addValue(value);
                assert !newNode.indexCore.keys.isEmpty();
                // assert
            }
            Core.forwardJoinRequest(msg);
            break;
        case D2TreeMessageT.JOIN_RES:
            Core.forwardJoinResponse(msg);
            isOnline = true;
            break;
        case D2TreeMessageT.LEAVE_REQ:
            this.forwardLeaveRequest(msg);
            break;
        // case D2TreeMessageT.LEAVE_RES:
        // Core.forwardLeaveResponse(msg);
        // isOnline = false;
        // break;
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
            ExtendRequest data = (ExtendRequest) msg.getData();
            int counter = msg.getHops();

            // this is a bucket node
            long oldOptimalBucketSize = data.getOldOptimalBucketSize();
            // trick, accounts for odd vs even optimal sizes
            long optimalBucketSize = (oldOptimalBucketSize - 1) / 2;
            if (counter == optimalBucketSize + 1 && data.buildsLeftLeaf()) {
                long leftLeaf = msg.getSourceId();
                long oldLeaf = Core.getRT().get(Role.REPRESENTATIVE);

                // move all the keys of this node to the old leaf
                KeyReplacementRequest kRepData = new KeyReplacementRequest(Id,
                        oldLeaf);
                indexCore.forwardKeyReplacementRequest(new Message(leftLeaf,
                        Id, kRepData), Core.getRT());

                // tell old leaf to move all its keys to the new left leaf
                KeyReplacementRequest kRepData2 = new KeyReplacementRequest(
                        oldLeaf, leftLeaf);
                send(new Message(Id, oldLeaf, kRepData2));

                // tell left leaf to move all its keys here
                KeyReplacementRequest kRepData3 = new KeyReplacementRequest(
                        leftLeaf, Id);
                send(new Message(Id, leftLeaf, kRepData3));
            }
            Core.forwardExtendRequest(msg);
            break;
        case D2TreeMessageT.EXTEND_RES:
            Core.forwardExtendResponse(msg);
            break;
        case D2TreeMessageT.CONTRACT_REQ:
            Core.forwardContractRequest(msg);
            break;
        case D2TreeMessageT.TRANSFER_REQ:
            long oldRepresentative = Core.getRT().get(Role.REPRESENTATIVE);
            TransferRequest transfData = (TransferRequest) msg.getData();
            long pivotBucket = transfData.getPivotBucket();
            long destBucket = transfData.getDestBucket();
            Core.forwardTransferRequest(msg);
            long newRepresentative = Core.getRT().get(Role.REPRESENTATIVE);
            if (newRepresentative != oldRepresentative) {
                if (newRepresentative == pivotBucket) {
                    KeyReplacementRequest keyRepData = new KeyReplacementRequest(
                            destBucket, Id, Mode.INORDER, true);
                    keyRepData.setKeys(new TreeSet<Long>(indexCore.keys));
                    indexCore.legacyKeys = new TreeSet<Long>(indexCore.keys);
                    indexCore.keys.clear();
                    send(new Message(Id, destBucket, keyRepData));
                    // indexCore.forwardKeyReplacementRequest(new Message(Id,
                    // destBucket, keyRepData), Core.getRT());
                }
                else if (newRepresentative == destBucket) {
                    KeyReplacementRequest keyRepData = new KeyReplacementRequest(
                            pivotBucket, Id, Mode.REVERSE_INORDER, true);
                    keyRepData.setKeys(new TreeSet<Long>(indexCore.keys));
                    indexCore.legacyKeys = new TreeSet<Long>(indexCore.keys);
                    indexCore.keys.clear();

                    send(new Message(Id, pivotBucket, keyRepData));
                    // indexCore.forwardKeyReplacementRequest(new Message(Id,
                    // pivotBucket, keyRepData), Core.getRT());
                }
                else assert false;
            }
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
            indexCore.lookupResponse();
            break;
        case D2TreeMessageT.DELETE_REQ:
            indexCore.lookup(msg, Core.getRT());
            break;
        case D2TreeMessageT.DELETE_RES:
            indexCore.lookupResponse();
            break;
        case D2TreeMessageT.INSERT_REQ:
            indexCore.lookup(msg, Core.getRT());
            break;
        case D2TreeMessageT.INSERT_RES:
            indexCore.lookupResponse();
            break;
        case D2TreeMessageT.PRINT_MSG:
            Core.printTree(msg);
            break;
        case D2TreeMessageT.REPLACE_KEY_REQ:
            indexCore.forwardKeyReplacementRequest(msg, Core.getRT());
            break;
        default:
            System.out.println("Unrecognized message type: " + mType);
            logger.logp(Level.SEVERE, this.getClass().getName(), "resolveMsg",
                    "Bad message");
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
        send(msg);
    }

    @Override
    public void leavePeer() {

        // find replacement node

        // migrate data

        // switch links

        Message msg = new Message(Id, Id, new LeaveRequest(Id));
        this.forwardLeaveRequest(msg);

        // send(msg);
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
    public void registerLogger(Logger logger) {
        this.logger = logger;
    }

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
        send(new Message(Id, nextNode, data));

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

    private static long getMinimumKeyBound(long precedingNodeID) {
        D2Tree precedingNode = allNodes.get(precedingNodeID);
        long minValue = D2TreeIndexCore.MIN_VALUE;
        if (precedingNode != null && !precedingNode.indexCore.keys.isEmpty()) {
            // if (!precedingNode.Core.isLeaf() &&
            // !precedingNode.Core.isBucketNode()) {
            // long ppNodeID = precedingNode.getPrecedingNode();
            // long psNodeID = precedingNode.getSucceedingNode();
            // if (precedingNode.needsSorting(ppNodeID, psNodeID)) {
            // precedingNode = allNodes.get(ppNodeID);
            // }
            // }
            minValue = Collections.max(precedingNode.indexCore.keys);
        }
        return minValue;
    }

    private static long getMaximumKeyBound(long succeedingNodeID) {
        D2Tree succeedingNode = allNodes.get(succeedingNodeID);
        long maxValue = D2TreeIndexCore.MAX_VALUE;
        if (succeedingNode != null && !succeedingNode.indexCore.keys.isEmpty()) {
            // if (!succeedingNode.Core.isLeaf() &&
            // !succeedingNode.Core.isBucketNode()) {
            // long spNodeID = succeedingNode.getPrecedingNode();
            // long ssNodeID = succeedingNode.getSucceedingNode();
            // if (succeedingNode.needsSorting(spNodeID, ssNodeID)) {
            // succeedingNode = allNodes.get(ssNodeID);
            // }
            // }
            maxValue = Collections.min(succeedingNode.indexCore.keys);
        }
        return maxValue;
    }

    static long generateRandomHandyValue(long precedingNodeID,
            long succeedingNodeID) {
        long minValue = getMinimumKeyBound(precedingNodeID);
        long maxValue = getMaximumKeyBound(succeedingNodeID);

        if (maxValue < minValue) {
            // D2Tree pNode = allNodes.get(precedingNodeID);
            // D2Tree sNode = allNodes.get(succeedingNodeID);
            try {
                // ArrayList<Double> pKeys = pNode.indexCore.keys;
                // ArrayList<Double> sKeys = sNode.indexCore.keys;
                throw new Exception("max: " + maxValue + " at succeedingNode " +
                        succeedingNodeID + ", min: " + minValue +
                        " at precedingNode " + precedingNodeID);
            }
            catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        assert maxValue >= minValue;
        long diff = maxValue - minValue;
        long generatedNumber = (long) (Math.random() * diff + minValue);

        if (minValue > generatedNumber || maxValue < generatedNumber) {
            String text = String.format(
                    "Generated Number %d not between %d and %d",
                    generatedNumber, minValue, maxValue);
            System.out.println(text);
        }
        assert minValue <= maxValue;
        assert generatedNumber >= minValue;
        assert generatedNumber <= maxValue;
        return generatedNumber;
    }

    static ArrayList<Double> generateRandomHandyValues(long precedingNodeID,
            long succeedingNodeID, int times) {
        double minValue = getMinimumKeyBound(precedingNodeID);
        double maxValue = getMaximumKeyBound(succeedingNodeID);

        if (maxValue < minValue) {
            // D2Tree pNode = allNodes.get(precedingNodeID);
            // D2Tree sNode = allNodes.get(succeedingNodeID);
            try {
                throw new Exception("max: " + maxValue + " at succeedingNode " +
                        succeedingNodeID + ", min: " + minValue +
                        " at precedingNode " + precedingNodeID);
            }
            catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        assert maxValue > minValue;
        double diff = maxValue - minValue;
        ArrayList<Double> generatedNumbers = new ArrayList<Double>();
        if (times < 1) times = 1;
        for (int i = 0; i < times; i++)
            generatedNumbers.add(Math.random() * diff + minValue);

        assert !generatedNumbers.isEmpty();
        return generatedNumbers;
    }

    long getPrecedingNode() {
        RoutingTable rt = Core.getRT();
        long pNodeID = RoutingTable.DEF_VAL;
        if (!Core.isLeaf() && !Core.isBucketNode()) {
            pNodeID = rt.get(Role.LEFT_A_NODE);
            if (pNodeID != RoutingTable.DEF_VAL) {
                D2Tree pNode = allNodes.get(pNodeID);

                // assert pNode.Core.isLeaf();

                RoutingTable pNodeRT = pNode.Core.getRT();
                if (pNodeRT.contains(Role.LAST_BUCKET_NODE)) {
                    pNodeID = pNodeRT.get(Role.LAST_BUCKET_NODE);
                }
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
        // assert minAllowedValue <= maxAllowedValue;
        if (minAllowedValue > maxAllowedValue) {
            try {
                throw new Exception("max: " + maxAllowedValue +
                        " at succeedingNode " + sNodeID + ", min: " +
                        minAllowedValue + " at precedingNode " + pNodeID);
            }
            catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        double minValue = indexCore.keys.isEmpty() ? D2TreeIndexCore.MIN_VALUE
                : Collections.min(indexCore.keys);
        double maxValue = indexCore.keys.isEmpty() ? D2TreeIndexCore.MAX_VALUE
                : Collections.max(indexCore.keys);
        // assert minValue < maxValue;
        if (minValue > maxValue) {
            try {
                throw new Exception("max: " + maxValue + ", min: " + minValue +
                        " at node " + this.Id);
            }
            catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        if (minValue < minAllowedValue || maxValue > maxAllowedValue) return true;
        else return false;
    }

    static LinkedHashMap<Long, TreeSet<Long>> getAllKeys() {
        LinkedHashMap<Long, TreeSet<Long>> keys = new LinkedHashMap<Long, TreeSet<Long>>();
        for (D2Tree peer : allNodes.values()) {
            keys.put(peer.Id, peer.indexCore.keys);
        }
        return keys;
    }

    void send(Message msg) {
        assert msg.getDestinationId() != this.Id;
        assert msg.getDestinationId() != RoutingTable.DEF_VAL;
        Net.sendMsg(msg);
    }

    void initializePBT() {

    }

    void initializeBuckets() {

    }
}
