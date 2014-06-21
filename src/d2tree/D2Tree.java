package d2tree;

import java.io.IOException;
import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
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

    final static int                     minHeight = 5;

    private static ArrayList<Long>       initialKeys;
    private static ArrayList<Long>       adjacencies;

    private long                         n;
    private long                         k;

    private int getRandomIntroducer() {
        randomGenerator = new Random();
        if (introducers.isEmpty()) return (int) RoutingTable.DEF_VAL;
        int index = randomGenerator.nextInt(D2Tree.introducers.size());
        return D2Tree.introducers.get(index);
    }

    @Override
    public void init(long id, long n, long k, Network Net) {

        this.Net = Net;
        this.Id = id;
        this.n = n;
        this.k = k;
        this.isOnline = false;
        this.state = Thread.State.NEW;
        this.Core = new D2TreeCore(Id, Net);
        this.indexCore = new D2TreeIndexCore(Id, Net);

        if (D2Tree.introducers == null)
            D2Tree.introducers = new ArrayList<Integer>();
        // if (D2Tree.introducers.isEmpty()) D2Tree.introducers.add(1);

        if (allNodes == null) allNodes = new HashMap<Long, D2Tree>();
        allNodes.put(Id, this);

        int minPBTNodes = (int) Math.pow(2, minHeight) - 1;
        int minLeaves = (int) Math.pow(2, minHeight - 1);

        if (k < n) k = n;
        long averageKeySpace = k / n;
        // TreeSet<Long> fullKeyset = new TreeSet<Long>();
        if (initialKeys == null) {
            Random rand = new Random();
            TreeSet<Long> tempKeyList = new TreeSet<Long>();
            while (tempKeyList.size() < n * averageKeySpace) {
                // fullKeyset.add((long) Math.random() * diff + minValue);
                tempKeyList.add(rand.nextLong());
            }
            initialKeys = new ArrayList<Long>(tempKeyList);
            // for (int i = 0; i < fullKeyset.size(); i += partitionSize) {
            // partitions.add(originalList.subList(i,
            // i + Math.min(partitionSize, originalList.size() - i)));
            // }
        }

        if (id <= minPBTNodes) {
            isOnline = true;
            if (adjacencies == null) {
                adjacencies = new ArrayList<Long>();
                adjacencies.add(Id);
            }
            // we initialize some nodes for the tree structure (2^h - 1)
            initializePBT(minPBTNodes, averageKeySpace);
            if (id == minPBTNodes) {

                PrintMessage data = new PrintMessage(D2TreeMessageT.JOIN_REQ,
                        id);
                LinkedHashMap<Long, RoutingTable> RTs = new LinkedHashMap<Long, RoutingTable>(
                        D2TreeCore.routingTables);
                try {
                    PrintMessage.printPBT(new Message(id, id, data), RTs,
                            "tree.txt");
                }
                catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                System.out.println("PBT Initialization completed");
            }
            return;
        }
        else if (id <= minLeaves * minHeight + minPBTNodes) {
            // the rest 192 (32 * 6) get into the buckets of the 32 resulting
            // leaves, 6 in each bucket
            initializeBuckets(minPBTNodes, minLeaves * minHeight,
                    averageKeySpace);
            isOnline = true;
            if (Id == minPBTNodes + minLeaves * minHeight) {
                System.out.println("Bucket initialization complete.");
                PrintMessage data = new PrintMessage(D2TreeMessageT.JOIN_REQ,
                        Id);
                if (D2Tree.introducers.isEmpty()) D2Tree.introducers.add(1);
                // PrintMessage.printTreeByIndex(allNodes, msg, logFile);
                Core.printTree(new Message(id, id, data));
            }
            return;
        }
        else {
            TreeSet<Long> values = D2Tree.generateRandomValues(n, k);
            indexCore.keys.addAll(values);
            assert !indexCore.keys.isEmpty();
        }
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
        // if (!isOnline && Id < 2 * D2Tree.minHeight - 1) return;
        switch (mType) {
        case D2TreeMessageT.JOIN_REQ:
            int minPBTNodes = (int) Math.pow(2, minHeight) - 1;
            int minLeaves = (int) Math.pow(2, minHeight - 1);
            int minBucketNodes = minLeaves * minHeight;
            if (msg.getSourceId() <= minPBTNodes + minBucketNodes) {
                isOnline = true;
                break;
            }
            // if (Core.isLeaf()) {
            // D2Tree newNode = allNodes.get(msg.getSourceId());
            // TreeSet<Long> values = D2Tree.generateRandomValues(n, k);
            // newNode.indexCore.keys.addAll(values);
            // assert !newNode.indexCore.keys.isEmpty();
            // // assert
            // }
            Core.forwardJoinRequest(msg);
            break;
        case D2TreeMessageT.JOIN_RES:
            if (isOnline || Id >= D2Tree.minHeight * D2Tree.minHeight - 1) {
                Core.forwardJoinResponse(msg);
                isOnline = true;
            }
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
                    // indexCore.forwardKeyReplacementRequest(new
                    // Message(Id,
                    // destBucket, keyRepData), Core.getRT());
                }
                else if (newRepresentative == destBucket) {
                    KeyReplacementRequest keyRepData = new KeyReplacementRequest(
                            pivotBucket, Id, Mode.REVERSE_INORDER, true);
                    keyRepData.setKeys(new TreeSet<Long>(indexCore.keys));
                    indexCore.legacyKeys = new TreeSet<Long>(indexCore.keys);
                    indexCore.keys.clear();

                    send(new Message(Id, pivotBucket, keyRepData));
                    // indexCore.forwardKeyReplacementRequest(new
                    // Message(Id,
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

        int minPBTNodes = (int) Math.pow(2, minHeight) - 1;
        int minLeaves = (int) Math.pow(2, minHeight - 1);

        long nodeTotal = minPBTNodes + minLeaves * minHeight;
        if (Id < nodeTotal) return;
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

    static TreeSet<Long> generateRandomValues(long nodespace, long keyspace) {
        long times = keyspace / nodespace;
        TreeSet<Long> generatedNumbers = new TreeSet<Long>();
        if (times < 1) times = 1;

        Random r = new Random();
        for (int i = 0; i < times; i++)
            generatedNumbers.add(r.nextLong());

        assert !generatedNumbers.isEmpty();
        return generatedNumbers;
    }

    static long generateRandomHandyValue(long precedingNodeID,
            long succeedingNodeID) {
        long minValue = getMinimumKeyBound(precedingNodeID);
        long maxValue = getMaximumKeyBound(succeedingNodeID);

        if (maxValue < minValue) {
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

    void initializePBT(long minPBTNodes, long averageKeySpace) {
        // if (Id == 1) return;
        Message mmm = new Message(Id, Id, new PrintMessage(
                D2TreeMessageT.JOIN_REQ, Id));
        PrintMessage.print(mmm, "initializing node " + Id, "connect.txt");

        System.out.println("initializing node " + Id);

        RoutingTable rt = new RoutingTable(Id);
        int nodeIndex = adjacencies.indexOf(Id);

        /*
         * set parent
         */
        if (Id != 1) rt.set(Role.PARENT, Math.floorDiv(Id, 2));

        if (Id * 2 <= minPBTNodes) {
            /*
             * set children
             */
            long lChild = Id * 2;
            rt.set(Role.LEFT_CHILD, lChild);

            long rChild = lChild + 1;
            rt.set(Role.RIGHT_CHILD, rChild);

            if (!D2Tree.adjacencies.contains(lChild))
                D2Tree.adjacencies.add(nodeIndex, lChild);
            nodeIndex = adjacencies.indexOf(Id);
            if (!D2Tree.adjacencies.contains(rChild))
                D2Tree.adjacencies.add(nodeIndex + 1, rChild);

            nodeIndex = adjacencies.indexOf(Id);
        }

        /*
         * set left adjacent
         */
        long lAdj = findLAdj(minPBTNodes);
        if (lAdj != RoutingTable.DEF_VAL) {
            rt.set(Role.LEFT_A_NODE, lAdj);
            if (!D2Tree.adjacencies.contains(lAdj))
                D2Tree.adjacencies.add(nodeIndex, lAdj);
        }

        nodeIndex = adjacencies.indexOf(Id);
        /*
         * set right adjacent
         */
        long rAdj = findRAdj(minPBTNodes, lAdj);
        if (rAdj != RoutingTable.DEF_VAL) {
            rt.set(Role.RIGHT_A_NODE, rAdj);
            if (!D2Tree.adjacencies.contains(rAdj))
                D2Tree.adjacencies.add(nodeIndex + 1, rAdj);
        }

        /*
         * set leftRT
         */

        long leftmostNodeLevel = Integer.highestOneBit((int) Id);
        for (int i = 0;; i++) {
            int j = (int) Math.pow(2, i);

            if (Id - j < leftmostNodeLevel) break;
            rt.set(Role.LEFT_RT, i, Id - j);
        }

        /*
         * set rightRT
         */
        long rightmostNodeLevel = leftmostNodeLevel * 2 - 1;
        for (int i = 0;; i++) {
            int j = (int) Math.pow(2, i);

            if (Id + j > rightmostNodeLevel) break;
            rt.set(Role.RIGHT_RT, i, Id + j);
        }

        long leftmostLeaf = (minPBTNodes + 1) / 2;
        if (Id >= leftmostLeaf) {
            int bucketSize = D2Tree.minHeight;
            long bucketIndex = Id - leftmostLeaf;
            /*
             * set first bucket node
             */
            long firstBucketNode = minPBTNodes + 1 + bucketIndex * bucketSize;
            rt.set(Role.FIRST_BUCKET_NODE, firstBucketNode);

            /*
             * set last bucket node
             */
            long lastBucketNode = firstBucketNode + bucketSize - 1;
            rt.set(Role.LAST_BUCKET_NODE, lastBucketNode);
        }

        this.indexCore.keys = findCorrespondingKeys(minPBTNodes,
                averageKeySpace);
        // if (Id != 1)
        Core.setRT(rt);
    }

    private long findLAdj(long minPBTNodes) {

        long leftmostLeaf = (minPBTNodes + 1) / 2;
        long lAdj = RoutingTable.DEF_VAL;

        if (Id > leftmostLeaf) {
            // this is a leaf and left adjacent is an inner node
            // left adjacent

            // long leafIndex = Id - leftmostLeaf;
            // lAdj = adjacencies.get(2 * (int) leafIndex - 1);

            int index = adjacencies.indexOf(Id);
            if (index != 0) {
                assert index > 0;
                lAdj = adjacencies.get(index - 1);
            }
        }
        else if (Id < leftmostLeaf) {
            // this is an inner node and left adjacent is a leaf
            lAdj = 2 * Id; // begin with the left child
            while (lAdj < leftmostLeaf) {
                lAdj = 2 * lAdj + 1;
            }
        }

        return lAdj;
    }

    private long findRAdj(long minPBTNodes, long lAdj) {
        long rightmostLeaf = minPBTNodes;
        long rAdj = RoutingTable.DEF_VAL;
        if (lAdj > Id) {
            rAdj = lAdj + 1;
        }
        else if (Id < rightmostLeaf) {
            rAdj = adjacencies.get(adjacencies.indexOf(Id) + 1);
        }
        return rAdj;
    }

    void initializeBuckets(long minPBTNodes, long minBucketNodes,
            long averageKeySpace) {
        RoutingTable rt = new RoutingTable(Id);

        long leftmostLeaf = (minPBTNodes + 1) / 2;
        long bucketSize = D2Tree.minHeight;

        System.out.println("initializing bucket node " + Id);

        /*
         * set left rt
         */
        long leftmostBucketNode = minPBTNodes + 1;
        long bucketIndex = (Id - leftmostBucketNode) / bucketSize;

        long firstBucketNode = leftmostBucketNode + bucketIndex * bucketSize;
        if (Id > firstBucketNode) rt.set(Role.LEFT_RT, 0, Id - 1);

        /*
         * set right rt
         */
        long lastBucketNode = firstBucketNode + bucketSize - 1;
        if (Id < lastBucketNode) rt.set(Role.RIGHT_RT, 0, Id + 1);

        /*
         * set representative
         */
        long repr = leftmostLeaf + bucketIndex;
        rt.set(Role.REPRESENTATIVE, repr);

        indexCore.keys = findCorrespondingKeys(minPBTNodes, averageKeySpace);
        Core.setRT(rt);
    }

    private TreeSet<Long> findCorrespondingKeys(long minPBTNodes,
            long averageKeySpace) {

        TreeSet<Long> keys = new TreeSet<Long>();
        long leftmostNodeLevel = Integer.highestOneBit((int) Id);
        long nodeIndexAtLevel = Id - leftmostNodeLevel;
        long treeHeight = D2Tree.minHeight;
        long bucketSize = treeHeight;

        if (Id <= minPBTNodes) {
            long nodeLevel = (long) (Math.log(leftmostNodeLevel) / Math.log(2)) + 1;
            long nodeLevelDistanceFromBottom = treeHeight - nodeLevel;

            // nodes of this level appear every nodeInterval nodes
            long nodeInterval = (long) Math.pow(2,
                    nodeLevelDistanceFromBottom + 1);

            long nodeInorderIndex = nodeInterval / 2 * (nodeIndexAtLevel + 1) -
                    1;

            long bucketNodesBetween = (nodeInorderIndex + 1) / 2 * bucketSize;

            long totalNodeIndex = nodeInorderIndex + bucketNodesBetween;
            if (Id > minPBTNodes + 1) totalNodeIndex++;

            long keysStartingIndex = totalNodeIndex * averageKeySpace;

            List<Long> temp = initialKeys.subList((int) keysStartingIndex,
                    (int) keysStartingIndex + (int) averageKeySpace);
            keys.addAll(temp);
        }
        else {
            long bucketIndexLevel = nodeIndexAtLevel / bucketSize;
            long bucketInorderIndex = bucketIndexLevel * 2;

            long offsetInBucket = nodeIndexAtLevel % bucketSize;
            long bucketNodesBetween = bucketIndexLevel * bucketSize +
                    offsetInBucket;

            long totalBucketIndex = bucketInorderIndex + bucketNodesBetween;

            long keysStartingIndex = totalBucketIndex * averageKeySpace;

            List<Long> temp = initialKeys.subList((int) keysStartingIndex,
                    (int) keysStartingIndex + (int) averageKeySpace);
            keys.addAll(temp);

        }
        return keys;
    }
}
