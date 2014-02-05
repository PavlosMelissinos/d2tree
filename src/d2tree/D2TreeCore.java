package d2tree;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import p2p.simulator.message.Message;
import p2p.simulator.network.Network;
import d2tree.RoutingTable.Role;
import d2tree.TransferResponse.TransferType;

public class D2TreeCore {
    private static List<D2TreeCore>    peers;
    static HashMap<Long, RoutingTable> routingTables;

    private RoutingTable               rt;
    private Network                    net;
    protected long                     id;
    private HashMap<Key, Long>         storedMsgData;
    // private HashMap<Key, Long> redistData;
    private Mode                       mode;
    private String                     printText;
    private RedistributionCore         redistCore;

    private static enum Key {
        LEFT_CHILD_SIZE,
        RIGHT_CHILD_SIZE,
        UNEVEN_CHILD,
        BUCKET_SIZE;
        // UNEVEN_SUBTREE_ID,
        // UNCHECKED_BUCKET_NODES,
        // UNCHECKED_BUCKETS,
        // DEST,
        // SURPLUS;
    }

    public static enum Mode {
        NORMAL("Normal Mode"),
        CHECK_BALANCE("Check Balance Mode"),
        REDISTRIBUTION("Redistribution Mode"),
        REDISTRIBUTION_PREPARE("Redistribution Setup Mode"),
        EXTEND("Extension Mode"),
        CONTRACT("Contraction Mode"),
        TRANSFER("Transfer Mode");
        private String name;

        Mode(String name) {
            this.name = name;
        }

        public String toString() {
            return name;
        }
    }

    private static enum ECMode {
        EXTEND("Extension Mode"),
        CONTRACT("Contraction Mode");
        private String name;

        ECMode(String name) {
            this.name = name;
        }

        public String toString() {
            return name;
        }
    }

    // TODO keep as singleton for simplicity, convert to Vector of keys later
    // private long key;

    D2TreeCore(long id, Network net) {
        this.rt = new RoutingTable(id);
        this.net = net;
        this.id = id;
        storedMsgData = new HashMap<Key, Long>();
        // redistData = new HashMap<Key, Long>();
        redistCore = new RedistributionCore();
        this.mode = Mode.NORMAL;
        if (peers == null)
            peers = Collections.synchronizedList(new ArrayList<D2TreeCore>());
        peers.add(this);
        if (routingTables == null)
            routingTables = new HashMap<Long, RoutingTable>();
        routingTables.put(id, this.rt);
    }

    /**
     * if core is leaf, then forward to first bucket node if exists, otherwise
     * connect node else if core is an inner node, then forward to nearest leaf
     * else if core is a bucket node, then forward to next bucket node until
     * it's the last bucket node of the bucket else if core is the last bucket
     * node, then connect node
     * **/
    void forwardJoinRequest(Message msg) {
        assert msg.getData() instanceof JoinRequest;
        long newNodeId = msg.getSourceId();

        assert !isBucketNode();
        if (isLeaf()) {
            if (mode == Mode.REDISTRIBUTION || mode == Mode.TRANSFER) {
                printText = "Bucket is busy. Resending message to a neighbor.";
                print(msg, printText, newNodeId);
                msg.setDestinationId(rt.getRandomRTNode());
                send(msg);
                return;
            }

            long bucketSize = 0;
            long lastBucketNode = RoutingTable.DEF_VAL;

            if (!rt.contains(Role.FIRST_BUCKET_NODE)) {
                // the leaf doesn't have any nodes in its bucket
                this.rt.set(Role.FIRST_BUCKET_NODE, newNodeId);
            }
            else {
                if (!this.storedMsgData.containsKey(Key.BUCKET_SIZE)) {
                    msg.setDestinationId(rt.getRandomRTNode());
                    send(msg);
                    return;
                }
                bucketSize = this.storedMsgData.get(Key.BUCKET_SIZE);
                lastBucketNode = rt.get(Role.LAST_BUCKET_NODE);

                if (lastBucketNode == RoutingTable.DEF_VAL) {
                    printText = "Routing table is incomplete. Resending message.";
                    print(msg, printText, newNodeId);
                    send(msg);
                    return;
                }

                // set the new node as the right neighbor of the bucket's last
                // node
                ConnectMessage connData = new ConnectMessage(newNodeId,
                        Role.RIGHT_RT, 0, false, lastBucketNode);
                send(new Message(id, lastBucketNode, connData));
            }

            if (mode == Mode.REDISTRIBUTION_PREPARE)
                redistCore.increaseSurplus();

            JoinResponse joinData = new JoinResponse(lastBucketNode);
            send(new Message(id, newNodeId, joinData));

            assert bucketSize != RoutingTable.DEF_VAL;
            this.storedMsgData.put(Key.BUCKET_SIZE, bucketSize + 1);
            this.rt.set(Role.LAST_BUCKET_NODE, newNodeId);

            printText = "Performing balance check...";
            this.print(msg, newNodeId);

            CheckBalanceRequest data = new CheckBalanceRequest(
                    this.storedMsgData.get(Key.BUCKET_SIZE), newNodeId);
            forwardCheckBalanceRequest(new Message(id, id, data));
        }
        else { // core is an inner node
            long destination = rt.get(Role.LEFT_A_NODE);
            if (destination == RoutingTable.DEF_VAL)
                destination = rt.get(Role.RIGHT_A_NODE);
            if (destination == RoutingTable.DEF_VAL)
                destination = rt.get(Role.LEFT_CHILD);
            if (destination == RoutingTable.DEF_VAL)
                destination = rt.get(Role.RIGHT_CHILD);
            if (destination == RoutingTable.DEF_VAL &&
                    !rt.isEmpty(Role.LEFT_RT))
                destination = rt.get(Role.LEFT_RT, 0);
            if (destination == RoutingTable.DEF_VAL &&
                    !rt.isEmpty(Role.RIGHT_RT))
                destination = rt.get(Role.RIGHT_RT, 0);
            // destination = Math.random() < 0.5 ? rt.get(Role.LEFT_CHILD) :
            // rt.get(Role.RIGHT_CHILD);
            /*
             * printText = "Node " + id +
             * " is an inner node. Forwarding request to " + destination +
             * "...";
             */
            // this.print(msg, newNodeId);
            msg.setDestinationId(destination);
            send(msg);
        }
    }

    public void forwardJoinResponse(Message msg) {
        JoinResponse data = (JoinResponse) msg.getData();
        long lastBucketNode = data.getLastBucketNode();
        long leaf = msg.getSourceId();

        rt.set(Role.REPRESENTATIVE, leaf);
        if (lastBucketNode != RoutingTable.DEF_VAL)
            rt.set(Role.LEFT_RT, 0, lastBucketNode);

        printText = String.format(
                "Node %d has been added to the bucket of %d.", id, leaf);
        this.print(msg, id);
        printTree(new Message(id, id, new PrintMessage(msg.getType(), id)));
    }

    /***
     * get subtree size if it doesn't exist (left subtree size + right subtree
     * size) else if subtree is not balanced forward request to parent and send
     * size data as well else if subtree is balanced, redistribute child
     * 
     * @param msg
     * 
     */
    void forwardCheckBalanceRequest(Message msg) {
        assert msg.getData() instanceof CheckBalanceRequest;
        CheckBalanceRequest data = (CheckBalanceRequest) msg.getData();
        // if (mode != Mode.NORMAL && mode != Mode.CHECK_BALANCE) {
        if (mode != Mode.NORMAL && mode != Mode.CHECK_BALANCE) {
            printText = "The subtree of " + this.id + " is busy (" + mode +
                    "). Aborting...";
            this.print(msg, data.getInitialNode());
            if (this.isLeaf())
                storedMsgData.put(Key.BUCKET_SIZE, data.getTotalBucketSize());
            return;
        }
        if (this.isBucketNode()) printErr(new Exception(),
                data.getInitialNode());
        else if (this.isLeaf()) storedMsgData.put(Key.BUCKET_SIZE,
                data.getTotalBucketSize());
        else if (msg.getSourceId() == rt.get(Role.LEFT_CHILD)) this.storedMsgData
                .put(Key.LEFT_CHILD_SIZE, data.getTotalBucketSize());
        else if (msg.getSourceId() == rt.get(Role.RIGHT_CHILD)) this.storedMsgData
                .put(Key.RIGHT_CHILD_SIZE, data.getTotalBucketSize());
        else {
            printText = "Node " + this.id +
                    " is extending. This is the end of the line. Tough luck, sorry...";
            this.print(msg, data.getInitialNode());
            return;
        }
        Long leftSubtreeSize = this.storedMsgData.get(Key.LEFT_CHILD_SIZE);
        Long rightSubtreeSize = this.storedMsgData.get(Key.RIGHT_CHILD_SIZE);
        Long bucketSize = this.storedMsgData.get(Key.BUCKET_SIZE);
        if (this.isLeaf()) {
            if (bucketSize == null) {
                // we haven't accessed this leaf before, we need to compute the
                // node's size
                printText = "Node " + this.id +
                        " is leaf and root. Computing bucket size...";
                this.print(msg, data.getInitialNode());
                msg = new Message(id, id, new GetSubtreeSizeRequest(
                        Mode.CHECK_BALANCE, data.getInitialNode()));
                if (mode == Mode.NORMAL)
                    setMode(Mode.CHECK_BALANCE, data.getInitialNode());
                forwardGetSubtreeSizeRequest(msg);
            }
            else if (!isRoot()) {
                printText = "Node " + this.id +
                        " is a leaf. Forwarding balance check request to parent...";
                this.print(msg, data.getInitialNode());
                msg = new Message(id, rt.get(Role.PARENT),
                        new CheckBalanceRequest(data.getTotalBucketSize(),
                                data.getInitialNode()));
                send(msg);
            }
            else {
                printText = "Node " + this.id +
                        " is root and leaf. Performing extension test...";
                this.print(msg, data.getInitialNode());
                // this.redistData.put(UNCHECKED_BUCKET_NODES,
                // data.getTotalBucketSize());
                // this.redistData.put(Key.UNCHECKED_BUCKET_NODES, bucketSize);
                // this.redistData.put(Key.UNCHECKED_BUCKETS, 1L);
                msg = new Message(id, id, new ExtendContractRequest(bucketSize,
                        1, data.getInitialNode()));
                setMode(Mode.NORMAL, data.getInitialNode());
                this.forwardExtendContractRequest(msg);
            }
        }
        else if (leftSubtreeSize == null || rightSubtreeSize == null) {
            assert leftSubtreeSize != null || rightSubtreeSize != null;
            // this isn't a leaf and some subtree data is missing
            Message msg1 = msg;

            if (leftSubtreeSize == null) // get left subtree size
            msg = new Message(id, rt.get(Role.LEFT_CHILD),
                    new GetSubtreeSizeRequest(Mode.CHECK_BALANCE,
                            data.getInitialNode()));
            else this.storedMsgData.put(Key.UNEVEN_CHILD,
                    rt.get(Role.LEFT_CHILD));

            if (rightSubtreeSize == null) // get right subtree size
            msg = new Message(id, rt.get(Role.RIGHT_CHILD),
                    new GetSubtreeSizeRequest(Mode.CHECK_BALANCE,
                            data.getInitialNode()));
            else this.storedMsgData.put(Key.UNEVEN_CHILD,
                    rt.get(Role.RIGHT_CHILD));
            printText = msg.getDestinationId() == rt.get(Role.LEFT_CHILD) ? "left child..."
                    : "right child...";
            printText = "This node is missing subtree data. Sending size request to its " +
                    printText;
            this.print(msg1, data.getInitialNode());
            send(msg);
        }
        else if (isBalanced()) {
            long unevenChild = msg.getSourceId();
            if (msg.getSourceId() == id)
                this.storedMsgData.get(Key.UNEVEN_CHILD);
            boolean childrenAreLeaves = rt.get(Role.LEFT_A_NODE) == rt
                    .get(Role.LEFT_CHILD) ||
                    rt.get(Role.RIGHT_A_NODE) == rt.get(Role.RIGHT_CHILD);
            if (childrenAreLeaves) {
                printText = "This is a balanced inner node ( |" +
                        leftSubtreeSize + "| vs |" + rightSubtreeSize +
                        "| ). Children " + rt.get(Role.LEFT_CHILD) + " and " +
                        rt.get(Role.RIGHT_CHILD) +
                        " are leaves and are always balanced. Nothing to redistribute here...";
                this.print(msg, data.getInitialNode());
            }
            else {
                printText = "This is a balanced inner node ( |" +
                        leftSubtreeSize + "| vs |" + rightSubtreeSize +
                        "| ). Forwarding balance check request " +
                        "to uneven child with id = " + unevenChild + "...";
                this.print(msg, data.getInitialNode());
                // Key key = rt.get(Role.LEFT_CHILD) == unevenChild ?
                // Key.LEFT_CHILD_SIZE
                // : Key.RIGHT_CHILD_SIZE;
                // long totalSubtreeSize = this.storedMsgData.get(key);
                this.storedMsgData.remove(Key.LEFT_CHILD_SIZE);
                this.storedMsgData.remove(Key.RIGHT_CHILD_SIZE);
                msg = new Message(id, unevenChild,
                        new RedistributionSetupRequest(1, unevenChild,
                                data.getInitialNode()));
                send(msg);
                // setMode(Mode.REDISTRIBUTION);
            }
            this.storedMsgData.remove(Key.LEFT_CHILD_SIZE);
            this.storedMsgData.remove(Key.RIGHT_CHILD_SIZE);
        }
        else {
            this.storedMsgData.remove(Key.LEFT_CHILD_SIZE);
            this.storedMsgData.remove(Key.RIGHT_CHILD_SIZE);
            setMode(Mode.NORMAL, data.getInitialNode());
            long totalSubtreeSize = leftSubtreeSize + rightSubtreeSize;
            if (!isRoot()) {
                printText = "This is an unbalanced inner node ( |" +
                        leftSubtreeSize + "| vs |" + rightSubtreeSize +
                        "| ). Forwarding balance check request to parent...";
                this.print(msg, data.getInitialNode());
                msg = new Message(id, rt.get(Role.PARENT),
                        new CheckBalanceRequest(totalSubtreeSize,
                                data.getInitialNode()));
                send(msg);
            }
            else {
                printText = "This is the root and it's unbalanced ( |" +
                        leftSubtreeSize + "| vs |" + rightSubtreeSize +
                        "| ). Performing full tree redistribution...";
                this.print(msg, data.getInitialNode());
                msg = new Message(id, id, new RedistributionSetupRequest(1, id,
                        data.getInitialNode()));
                this.forwardBucketPreRedistributionRequest(msg);
            }
        }
    }

    public void forwardBucketPreRedistributionRequest(Message msg) {
        RedistributionSetupRequest data = (RedistributionSetupRequest) msg
                .getData();
        // if this is an inner node, then forward to right child
        if (this.isBucketNode())
            throw new UnsupportedOperationException("What are you doing here?");
        if (!this.isLeaf()) {
            if (data.getSubtreeID() == id)
                setMode(Mode.REDISTRIBUTION, data.getInitialNode());
            descendToLeaf(msg);
            return;
        }

        assert this.isLeaf();
        long bucketIndex = data.getBucketIndex();
        long pivotBucket = data.getPivotBucket();
        if (pivotBucket == RedistributionSetupRequest.DEF_VAL) {
            pivotBucket = id;
            msg = new Message(id, id, null);
        }

        redistCore.clear();
        mode = Mode.REDISTRIBUTION_PREPARE;
        long bucketSize = storedMsgData.get(Key.BUCKET_SIZE);
        long totalBucketNodes = data.getTotalBucketNodes() + bucketSize;
        long totalBuckets = data.getTotalBuckets();
        printText = String
                .format("Setting %d (index %d of total %d, |%d|) into redistribution mode. Total BNs are %d (+%d).",
                        id, bucketIndex, totalBuckets, bucketSize,
                        totalBucketNodes, bucketSize);
        if (bucketIndex < totalBuckets) {
            data.setPivotBucket(pivotBucket);
            data.update(bucketSize);
            msg.setDestinationId(rt.get(Role.LEFT_RT, 0));
            msg.setData(data);
            printText += String.format(" Forwarding to left neighbor %d.",
                    rt.get(Role.LEFT_RT, 0));
            send(msg);
        }
        else {
            RedistributionRequest rData = new RedistributionRequest(
                    totalBucketNodes, totalBuckets, data.getSubtreeID(),
                    data.getInitialNode());
            printText += String
                    .format("This is the last node of the subtree. Forwarding to pivotBucket %d.",
                            data.getPivotBucket());
            send(new Message(id, pivotBucket, rData));
        }
        this.print(msg, data.getInitialNode());
    }

    private void descendToLeaf(Message msg) {
        RedistributionSetupRequest data = (RedistributionSetupRequest) msg
                .getData();
        long totalBuckets = 2 * data.getTotalBuckets();
        long subtreeID = data.getSubtreeID();
        data = new RedistributionSetupRequest(totalBuckets, subtreeID,
                data.getInitialNode());
        long destination = rt.get(Role.RIGHT_CHILD);
        send(new Message(msg.getSourceId(), destination, data));
        printText = "Node " + id + " is an inner node. Forwarding request to " +
                destination + "(" + totalBuckets +
                " unchecked buckets for subtree " + subtreeID + ").";
        this.print(msg, data.getInitialNode());
    }

    /***
     * check if any nodes need to move and keep each bucket size at
     * subtreesize/totalBuckets or +1 (total buckets = h^2)
     */
    void forwardBucketRedistributionRequest(Message msg) {
        assert msg.getData() instanceof RedistributionRequest;
        RedistributionRequest data = (RedistributionRequest) msg.getData();
        try {
            setupRedistribution(msg);
        }
        catch (IllegalStateException ex) {
            return;
        }
        assert this.isLeaf();
        assert mode == Mode.REDISTRIBUTION;
        /*
         * now that the bucket is ready, check if any nodes need to be
         * transferred from/to it
         */
        long surplus = redistCore.getSurplus();
        Long bucketSize = this.storedMsgData.get(Key.BUCKET_SIZE) - surplus;
        // long uncheckedBucketNodes = redistCore.getUncheckedBucketNodes() +
        // surplus;
        long uncheckedBucketNodes = redistCore.getUncheckedBucketNodes();
        long uncheckedBuckets = redistCore.getUncheckedBuckets();

        long optimalBucketSize = uncheckedBucketNodes / uncheckedBuckets;
        long diff = bucketSize - optimalBucketSize;
        long pivotDiff = data.getPivotBucketSize() - optimalBucketSize;
        long dest = redistCore.getDest();
        long spareNodes = uncheckedBucketNodes % uncheckedBuckets;
        if (uncheckedBuckets == 1) {
            endRedistribution(msg, optimalBucketSize, diff);
            return;
        }
        if (diff == 0 || (diff == 1 && spareNodes > 0)) {
            if (dest != id) movePivot(msg);
            else moveDest(msg);
            return;
        }
        else if (dest == id) {
            /*
             * this bucket is dest and not ok, so send a response to the source
             * of this message
             */
            if (pivotDiff * diff > 0) {
                moveDest(msg);
                return;
            }
            printText = String
                    .format("Node %d is dest (data.transferDest is %d, redistData(Key.DEST) is %d. "
                            + "|%d| vs optimal |%d|, surplus is |%d|). "
                            + "Sending redistribution response to pivot bucket %d with size %d...",
                            id, data.getTransferDest(), redistCore.getDest(),
                            bucketSize, optimalBucketSize, surplus,
                            msg.getSourceId(), data.getPivotBucketSize());
            this.print(msg, data.getInitialNode());
            assert diff != 0;
            redistCore.setUncheckedBucketNodes(uncheckedBucketNodes);
            msg = new Message(id, msg.getSourceId(),
                    new RedistributionResponse(rt.get(Role.LAST_BUCKET_NODE),
                            bucketSize, uncheckedBucketNodes,
                            data.getInitialNode()));
            send(msg);
            setMode(Mode.TRANSFER, data.getInitialNode());
        }
        else if (dest != id) {
            // nodes probably need to be transferred from/to this node
            printText = "The bucket of node " + id +
                    " is larger than optimal by " + diff + " (" + bucketSize +
                    " vs " + optimalBucketSize + ", surplus is |" + surplus +
                    "|). Forwarding request to dest = " + dest + ".";
            this.print(msg, data.getInitialNode());
            setMode(Mode.TRANSFER, data.getInitialNode());
            if (dest == RedistributionRequest.DEF_VAL) {
                // this means we've just started the transfer process
                if (rt.isEmpty(Role.LEFT_RT)) {
                    System.err.println("");
                    String exceptionMsg = "Dest node is " + dest +
                            " so redistribution cannot move there from node " +
                            id + ".";
                    new Exception(exceptionMsg).printStackTrace();
                    PrintMessage printData = new PrintMessage(msg.getType(),
                            data.getInitialNode());
                    printTree(new Message(id, id, printData));
                }
                else redistCore.setDest(rt.get(Role.LEFT_RT, 0));
                dest = redistCore.getDest();
            }
            data.setTransferDest(dest);
            data.setPivotBucketSize(bucketSize);
            msg = new Message(id, dest, data);
            send(msg);
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

    private void setupRedistribution(Message msg) throws IllegalStateException {
        RedistributionRequest data = (RedistributionRequest) msg.getData();
        /*
         * if it's the first time we visit this leaf, we need to prepare it for
         * what is to come, that is compute the size of its bucket and set to
         * "redistribution" mode
         */
        Long bucketSize = this.storedMsgData.get(Key.BUCKET_SIZE);
        if (bucketSize == null) {
            printText = "Node " + id +
                    " is missing bucket info. Computing bucket size...";
            this.print(msg, data.getInitialNode());
            msg = new Message(id, rt.get(Role.FIRST_BUCKET_NODE),
                    new GetSubtreeSizeRequest(Mode.REDISTRIBUTION,
                            data.getInitialNode()));
            send(msg);
            throw new IllegalStateException(printText);
        }
        printText = "Initializing redistCore info, overwriting old values (skipping surplus info)...";
        long oldSurplus = redistCore.getSurplus();
        this.print(msg, data.getInitialNode());
        boolean keepSurplus = true;
        redistCore.clear(keepSurplus);
        redistCore.set(data);
        assert redistCore.getSurplus() == oldSurplus;
        if (data.getTransferDest() != RedistributionRequest.DEF_VAL) {
            redistCore.setDest(data.getTransferDest());
        }
        else {
            redistCore.setDest(rt.get(Role.LEFT_RT, 0));
        }
        setMode(Mode.REDISTRIBUTION, data.getInitialNode());
    }

    private void endRedistribution(Message msg, long optimalBucketSize,
            long diff) {
        // redistribution is over so check if the tree needs
        // extension/contraction
        RedistributionRequest data = (RedistributionRequest) msg.getData();
        // reset bucket data
        // TODO removed this to check if code works
        // this.redistCore.clear();
        setMode(Mode.NORMAL, data.getInitialNode());

        // end redistribution
        long subtreeID = data.getSubtreeID();
        long totalBuckets = new Double(Math.pow(2, rt.getHeight() - 1))
                .longValue();
        if (diff != 0) optimalBucketSize = optimalBucketSize - 1;
        long totalBucketNodes = optimalBucketSize * totalBuckets;

        printText = "The tree is balanced. Doing an extend/contract test...";
        this.print(msg, data.getInitialNode());
        ExtendContractRequest ecData = new ExtendContractRequest(
                totalBucketNodes, rt.getHeight(), data.getInitialNode());
        msg = new Message(id, subtreeID, ecData);
        send(msg);
    }

    private void movePivot(Message msg) {
        // this bucket is ok, so move to the next one (if there is one)

        RedistributionRequest data = (RedistributionRequest) msg.getData();

        // get all necessary info
        Long bucketSize = this.storedMsgData.get(Key.BUCKET_SIZE);
        Long uncheckedBucketNodes = data.getTotalUncheckedBucketNodes();
        Long uncheckedBuckets = data.getTotalUncheckedBuckets();
        long subtreeID = redistCore.getUnevenSubtreeID();

        // reset bucket status
        // TODO does it work without the following code?
        // this.redistCore.clear();
        setMode(Mode.NORMAL, data.getInitialNode());

        data = new RedistributionRequest(uncheckedBucketNodes - bucketSize,
                uncheckedBuckets - 1, subtreeID, data.getInitialNode());

        long leftNeighbor = rt.get(Role.LEFT_RT, 0);

        printText = "This bucket has the right size. Forwarding request to bucket " +
                leftNeighbor + " (" + uncheckedBuckets + " unchecked buckets).";
        this.print(msg, data.getInitialNode());
        msg = new Message(id, leftNeighbor, data);
        send(msg);
    }

    private void moveDest(Message msg) {
        /*
         * this bucket is dest at the right size, so forward request to its left
         * neighbor
         */
        RedistributionRequest data = (RedistributionRequest) msg.getData();
        long surplus = redistCore.getSurplus();
        Long bucketSize = this.storedMsgData.get(Key.BUCKET_SIZE) - surplus;
        long destIndex = data.getDestIndex();

        // reset bucket status except surplus counter
        boolean keepSurplus = true;
        redistCore.clear(keepSurplus);
        assert redistCore.getSurplus() == surplus;
        setMode(Mode.REDISTRIBUTION_PREPARE, data.getInitialNode());

        // data.setTotalUncheckedBucketNodes(data.getTotalUncheckedBucketNodes()
        // + surplus);
        data.setDestOffset(data.getDestOffset() + 1);

        long uncheckedBucketNodes = data.getTotalUncheckedBucketNodes();
        long uncheckedBuckets = data.getTotalUncheckedBuckets();
        // assert uncheckedBucketNodes != null;
        // assert uncheckedBuckets != null;

        // assert !redistData.isEmpty();
        long optimalBucketSize = uncheckedBucketNodes / uncheckedBuckets;

        if (destIndex < 0 || rt.isEmpty(Role.LEFT_RT)) {
            printText = "Node " + id + " is dest and is ok (size = " +
                    bucketSize + " vs " + optimalBucketSize +
                    ") but doesn't have any neighbors to its left." +
                    "Going back to pivot bucket with id = " +
                    msg.getSourceId() + "...";
            this.print(msg, data.getInitialNode());
            // redistData.remove(Key.DEST);
            msg = new Message(id, msg.getSourceId(), data);
            send(msg);
            return;
        }
        long newDest = rt.get(Role.LEFT_RT, 0);
        data.setTransferDest(newDest);
        msg.setDestinationId(newDest);
        msg.setData(data);
        printText = "Node " +
                id +
                " is dest and is ok (size = " +
                bucketSize +
                "). Forwarding redistribution request to left neighbor with id = " +
                rt.get(Role.LEFT_RT, 0) + " which is the new dest bucket...";
        this.print(msg, data.getInitialNode());
        send(msg);
    }

    void forwardBucketRedistributionResponse(Message msg) {
        assert msg.getData() instanceof RedistributionResponse;
        RedistributionResponse data = (RedistributionResponse) msg.getData();
        if (this.redistCore.isEmpty()) {
            System.err.println(rt.toString());
        }
        assert !this.redistCore.isEmpty();

        long destBucket = msg.getSourceId();
        long uncheckedBucketNodes = data.getUncheckedBucketNodes();
        long subtreeID = this.redistCore.getUnevenSubtreeID();
        long uncheckedBuckets = this.redistCore.getUncheckedBuckets();
        long optimalBucketSize = uncheckedBucketNodes / uncheckedBuckets;
        long spareNodes = uncheckedBucketNodes % uncheckedBuckets;
        long surplus = redistCore.getSurplus();
        long bucketSize = this.storedMsgData.get(Key.BUCKET_SIZE) - surplus;
        int diff = (int) (bucketSize - optimalBucketSize);
        long destSize = data.getDestSize() - surplus;
        int destDiff = (int) (destSize - optimalBucketSize);

        if (diff == 0 || (diff == 1 && spareNodes > 0)) {
            // pivot bucket is ok, so move to its left neighbor
            // TODO merge with movePivot
            // TODO is the following line the culprit?
            // redistCore.clear();
            long newPivotBucket = rt.get(Role.LEFT_RT, 0);
            RedistributionRequest rData = new RedistributionRequest(
                    uncheckedBucketNodes - bucketSize, uncheckedBuckets - 1,
                    subtreeID, data.getInitialNode());
            send(new Message(id, newPivotBucket, rData));
            printText = String
                    .format("Bucket %d is ok (|%d|, %d uncheckedBuckets). "
                            + "Sending a redistribution request to new pivot bucket %d.",
                            id, bucketSize, uncheckedBuckets, newPivotBucket);
            this.print(msg, data.getInitialNode());
            setMode(Mode.NORMAL, data.getInitialNode());
        }
        else if (diff * destDiff >= 0) {
            // both this bucket and dest have either more or less nodes (or
            // exactly the number we need)
            if ((destDiff == 0 || destDiff == 1)) printText = String
                    .format("Pivot bucket size = %d. Destbucket %d has the right size (%d). We're done here.",
                            bucketSize, destBucket, optimalBucketSize);
            else printText = String.format(
                    "Destbucket %d and pivotBucket are both too large/small ("
                            + "%d vs %d, instead of %d).", destBucket,
                    destSize, bucketSize, optimalBucketSize);

            printText += "Forwarding redistribution request to destBucket " +
                    destBucket + ".";

            this.print(msg, data.getInitialNode());
            RedistributionRequest rData = new RedistributionRequest(
                    uncheckedBucketNodes, uncheckedBuckets, subtreeID,
                    data.getInitialNode());
            rData.setTransferDest(destBucket);
            msg = new Message(id, destBucket, rData);

            send(msg);
        }
        else {
            assert diff * destDiff < 0;
            TransferRequest transfData = new TransferRequest(destBucket, id,
                    Math.abs(diff + destDiff), data.getInitialNode());
            if (diff > destDiff) { // |pivotBucket| > |destBucket|
                // move nodes from pivotBucket to destBucket
                printText = "Node " + id +
                        " has extra nodes that dest bucket " + destBucket +
                        " can use (" + bucketSize + " vs " + destSize + ")" +
                        " Performing transfer from bucket " + id +
                        " to bucket " + destBucket + ".";
                this.print(msg, data.getInitialNode());
                msg = new Message(data.getDestNode(),
                        rt.get(Role.FIRST_BUCKET_NODE), transfData);
            }
            else { // |pivotBucket| < |destBucket|
                   // move nodes from destBucket to pivotBucket
                printText = "Node " + id +
                        " is missing nodes that dest bucket " + destBucket +
                        " has in abundance." + " Performing transfer from " +
                        destBucket + " to " + id + ".";
                this.print(msg, data.getInitialNode());
                msg = new Message(rt.get(Role.FIRST_BUCKET_NODE),
                        data.getDestNode(), transfData);
                // msg = new Message(id, data.getDestNode(), transfData);
            }
            send(msg);
        }
    }

    void forwardTransferRequest(Message msg) {
        assert msg.getData() instanceof TransferRequest;
        TransferRequest transfData = (TransferRequest) msg.getData();
        long destBucket = transfData.getDestBucket();
        long pivotBucket = transfData.getPivotBucket();
        assert this.isBucketNode();
        assert !this.isLeaf();
        if (!this.isBucketNode() || this.isLeaf()) {
            String message = "Dest bucket: " + destBucket + ", pivot bucket: " +
                    pivotBucket + ", rt: " + rt;
            throw new IllegalArgumentException(message);
        }
        printText = "Moving " + id + " from bucket %d to bucket %d.";
        assert rt.get(Role.REPRESENTATIVE) == destBucket ||
                rt.get(Role.REPRESENTATIVE) == pivotBucket;
        if (rt.get(Role.REPRESENTATIVE) == destBucket) {
            // int transferSize = transfData.getPasses();
            // assert rt.isEmpty(Role.RIGHT_RT);

            // move transferSize nodes from the dest bucket to pivot
            long pivotNode = msg.getSourceId();
            long destNode = id;
            printText = String.format(printText, destBucket, pivotBucket);
            this.print(msg, transfData.getInitialNode());

            // remove the link from dest node's left neighbor to dest node
            DisconnectMessage discData = new DisconnectMessage(id,
                    Role.RIGHT_RT, 0, transfData.getInitialNode());
            send(new Message(id, rt.get(Role.LEFT_RT, 0), discData));

            // set left neighbor as the last bucket node of the bucket (dest)
            ConnectMessage connData = new ConnectMessage(
                    rt.get(Role.LEFT_RT, 0), Role.LAST_BUCKET_NODE, true,
                    transfData.getInitialNode());
            send(new Message(id, rt.get(Role.REPRESENTATIVE), connData));

            // set dest node as pivot node's left neighbor
            connData = new ConnectMessage(destNode, Role.LEFT_RT, 0, true,
                    transfData.getInitialNode());
            send(new Message(id, pivotNode, connData));

            // remove the link from dest node to its left neighbor
            rt.unset(Role.LEFT_RT);
            // set pivotNode as dest node's right neighbor
            rt.set(Role.RIGHT_RT, 0, pivotNode);

            // set pivotBucket as dest node's representative
            rt.set(Role.REPRESENTATIVE, pivotBucket);

            // set destNode as pivotBucket's first bucket node
            connData = new ConnectMessage(destNode, Role.FIRST_BUCKET_NODE,
                    true, transfData.getInitialNode());
            send(new Message(id, pivotBucket, connData));

            TransferResponse respData;
            respData = new TransferResponse(TransferType.NODE_REMOVED,
                    pivotBucket, transfData.getInitialNode());
            send(new Message(id, destBucket, respData));

            respData = new TransferResponse(TransferType.NODE_ADDED,
                    pivotBucket, transfData.getInitialNode());
            send(new Message(id, pivotBucket, respData));

            printText = "Successfully moved " + destNode + " next to " +
                    pivotNode + "...";
        }
        else {
            // move this node from the pivot bucket to dest
            long destNode = msg.getSourceId();
            long pivotNode = id;
            printText = String.format(printText, pivotBucket, destBucket);
            this.print(msg, transfData.getInitialNode());

            // remove the link from the right neighbor to this node
            DisconnectMessage discData = new DisconnectMessage(id,
                    Role.LEFT_RT, 0, transfData.getInitialNode());
            send(new Message(id, rt.get(Role.RIGHT_RT, 0), discData));

            // set right neighbor as the first bucket node of the bucket (pivot)
            ConnectMessage connData = new ConnectMessage(rt.get(Role.RIGHT_RT,
                    0), Role.FIRST_BUCKET_NODE, true,
                    transfData.getInitialNode());
            send(new Message(id, rt.get(Role.REPRESENTATIVE), connData));

            // set pivot node as dest node's right neighbor
            connData = new ConnectMessage(pivotNode, Role.RIGHT_RT, 0, true,
                    transfData.getInitialNode());
            send(new Message(id, destNode, connData));

            // remove the link from this node to its right neighbor
            rt.unset(Role.RIGHT_RT);

            // set dest node as pivot node's left neighbor
            rt.set(Role.LEFT_RT, 0, destNode);

            // set dest bucket as pivot node's representative
            rt.set(Role.REPRESENTATIVE, destBucket);

            // set pivotNode as destBucket's last bucket node
            connData = new ConnectMessage(pivotNode, Role.LAST_BUCKET_NODE,
                    true, transfData.getInitialNode());
            send(new Message(id, destBucket, connData));

            TransferResponse respData = new TransferResponse(
                    TransferType.NODE_REMOVED, pivotBucket,
                    transfData.getInitialNode());
            send(new Message(id, pivotBucket, respData));

            respData = new TransferResponse(TransferType.NODE_ADDED,
                    pivotBucket, transfData.getInitialNode());
            send(new Message(id, destBucket, respData));

            printText = "Successfully moved " + pivotNode + " next to " +
                    destNode + "...";
        }
        printText += " Transfer has been successful. THE END";
        // findRTInconsistencies(true);
        print(msg, transfData.getInitialNode());
        PrintMessage printData = new PrintMessage(msg.getType(),
                transfData.getInitialNode());
        printTree(new Message(id, id, printData));
    }

    void forwardTransferResponse(Message msg) {
        assert msg.getData() instanceof TransferResponse;
        TransferResponse data = (TransferResponse) msg.getData();
        if (this.isLeaf()) {
            TransferType transfType = data.getTransferType();
            long pivotBucket = data.getPivotBucket();
            long bucketSize = storedMsgData.get(Key.BUCKET_SIZE);
            bucketSize = transfType == TransferType.NODE_ADDED ? bucketSize + 1
                    : bucketSize - 1;
            storedMsgData.put(Key.BUCKET_SIZE, bucketSize);

            if (pivotBucket == id) return;

            Long destBucket = redistCore.getDest();
            boolean isDestBucket = destBucket != null && destBucket == id;
            if (isDestBucket) {
                mode = Mode.REDISTRIBUTION;
                printText = String
                        .format("Node %d is dest bucket. "
                                + "Forwarding redistribution response to pivot bucket with id = %d...",
                                id, pivotBucket);
                print(msg, data.getInitialNode());
                long uncheckedBucketNodes = redistCore
                        .getUncheckedBucketNodes();
                RedistributionResponse rData = new RedistributionResponse(
                        rt.get(Role.LAST_BUCKET_NODE), bucketSize,
                        uncheckedBucketNodes, data.getInitialNode());
                msg = new Message(id, pivotBucket, rData);
                send(msg);
            }
        }
        else throw new UnsupportedOperationException();
    }

    void forwardExtendContractRequest(Message msg) {
        assert msg.getData() instanceof ExtendContractRequest;

        ExtendContractRequest data = (ExtendContractRequest) msg.getData();

        setMode(Mode.NORMAL, data.getInitialNode());

        if (!this.isRoot()) {
            // We only extend and contract if the root is uneven.
            printText = "The tree is even. Extension/contraction is not required";
            print(msg, data.getInitialNode());
            // if (mode == Mode.REDISTRIBUTION)
            // setMode(Mode.NORMAL, data.getInitialNode());
            return;
        }

        // if (mode == Mode.REDISTRIBUTION)
        // setMode(Mode.EXTEND);
        long treeHeight = data.getHeight();
        long totalBucketNodes = data.getTotalBucketNodes();
        double totalBuckets = new Double(Math.pow(2, data.getHeight() - 1));
        double averageBucketSize = (double) totalBucketNodes / totalBuckets;
        printText = "Tree height is " + treeHeight +
                " and average bucket size is " + averageBucketSize +
                " (bucket nodes are " + totalBucketNodes + " and there are " +
                totalBuckets + " unchecked buckets). ";
        if (shouldExtend(treeHeight, averageBucketSize)) {
            printText += "Initiating tree extension.";
            print(msg, data.getInitialNode());
            // ExtendRequest eData = new
            // ExtendRequest(Math.round(optimalBucketSize),
            // (long)averageBucketSize, data.getInitialNode());
            ExtendRequest eData = new ExtendRequest((long) averageBucketSize,
                    true, data.getInitialNode(), getOptimalHeight(
                            (long) averageBucketSize, treeHeight), 1);
            msg = new Message(id, id, eData);
            // setMode(Mode.EXTEND);
            forwardExtendRequest(msg);
            // send(msg);
        }
        else if (shouldContract(treeHeight, averageBucketSize) &&
                !this.isLeaf()) {
            printText += "Initiating tree contraction.";
            print(msg, data.getInitialNode());
            // ContractRequest cData = new
            // ContractRequest(Math.round(optimalBucketSize),
            // data.getInitialNode());
            ContractRequest cData = new ContractRequest(
                    (long) averageBucketSize, data.getInitialNode());
            msg = new Message(id, id, cData);
            // setMode(Mode.CONTRACT);
            forwardContractRequest(msg);
            // send(msg);
        }
        else if (shouldContract(treeHeight, averageBucketSize) && this.isLeaf()) {
            printText += "Tree is already at minimum height. Can't contract any more.";
            print(msg, data.getInitialNode());
        }
        else {
            printText += "No action needed.";
            print(msg, data.getInitialNode());
            // if (mode == Mode.REDISTRIBUTION) mode = Mode.NORMAL;
        }
    }

    void forwardExtendRequest(Message msg) {
        assert msg.getData() instanceof ExtendRequest;
        ExtendRequest data = (ExtendRequest) msg.getData();
        long initialNode = data.getInitialNode();

        // travel to leaves of the tree, if not already there
        if (this.isLeaf()) {
            // forward request to the bucket node
            long optimalHeight = data.getOptimalHeight();

            Long bucketSize = this.storedMsgData.get(Key.BUCKET_SIZE);
            // long currentHeight = data.getCurrentHeight();
            long currentHeight = rt.getHeight();
            printText = "Node " + id + " is a leaf with size = " + bucketSize +
                    ". Optimal height = " + optimalHeight +
                    ", current height = " + currentHeight + ". ";
            if (mode == Mode.EXTEND) {
                printText += "Node is extending. No action needed.";
            }
            else if (mode == Mode.CONTRACT) {
                printText += "Node is contracting. No action needed.";
            }
            // else if (optimalHeight > msg.getHops()) {
            else if (optimalHeight > currentHeight) {
                printText += "Forwarding request to bucket node with id = " +
                        rt.get(Role.FIRST_BUCKET_NODE) + "...";
                data = new ExtendRequest(bucketSize, true, initialNode,
                        data.getOptimalHeight(), currentHeight + 1);
                send(new Message(msg.getSourceId(),
                        rt.get(Role.FIRST_BUCKET_NODE), data));
                setMode(Mode.EXTEND, data.getInitialNode());
            }
            else {
                printText += "Tree is already at the right height. No further action needed.";
                // this.print(msg, initialNode);
            }
            this.print(msg, initialNode);
            return;
        }
        else if (!this.isBucketNode()) { // forward request to the children
            printText = "Node " +
                    id +
                    " is an inner node. Forwarding request to children with id = " +
                    rt.get(Role.LEFT_CHILD) + " and " +
                    rt.get(Role.RIGHT_CHILD) + " respectively...";
            this.print(msg, data.getInitialNode());

            data.incrementHeight();
            // msg.setData(data);

            send(new Message(id, rt.get(Role.LEFT_CHILD), data));

            send(new Message(id, rt.get(Role.RIGHT_CHILD), data));
            return;
        }

        // this is a bucket node
        long oldOptimalBucketSize = data.getOldOptimalBucketSize();

        // trick, accounts for odd vs even optimal sizes
        long optimalBucketSize = (oldOptimalBucketSize - 1) / 2;
        int counter = msg.getHops();
        if (counter == 1 && data.buildsLeftLeaf()) {
            // this is the first node of the bucket, make it a left leaf
            printText = "Node " + id + " is the first node of bucket " +
                    rt.get(Role.REPRESENTATIVE) + " (index = " + counter +
                    "). Making it a left leaf...";
            rt.print(new PrintWriter(System.err));
            this.print(msg, data.getInitialNode());
            bucketNodeToLeftLeaf(msg);
        }
        else if (counter > optimalBucketSize + 1 && data.buildsLeftLeaf()) {
            // the left bucket is full, make this a right leaf forward extend
            // response to the old leaf
            long leftLeaf = msg.getSourceId();
            long rightLeaf = id;
            long oldLeaf = rt.get(Role.REPRESENTATIVE);

            // the left bucket is full, forward a new extend request to the new
            // (left) leaf

            printText = "Node " + id + " is the middle node of bucket " +
                    rt.get(Role.REPRESENTATIVE) + " (index = " + counter +
                    "). Left leaf is the node with id = " + leftLeaf +
                    " (bucket size = " + (counter - 2) +
                    ") and the old leaf has id = " + oldLeaf + ". Making " +
                    id + " a right leaf...";
            this.print(msg, data.getInitialNode());

            ExtendResponse exData = new ExtendResponse(0, leftLeaf, rightLeaf,
                    data.getInitialNode());
            send(new Message(id, oldLeaf, exData));

            oldLeaftoInnerNode(leftLeaf, rightLeaf, oldLeaf,
                    data.getInitialNode());

            // set the left neighbor as the last bucket node of the left child
            ConnectMessage conn = new ConnectMessage(rt.get(Role.LEFT_RT, 0),
                    Role.LAST_BUCKET_NODE, true, initialNode);
            send(new Message(id, leftLeaf, conn));
            bucketNodeToRightLeaf(msg);
        }
        else {
            long oldLeaf = rt.get(Role.REPRESENTATIVE);
            long newLeaf = msg.getSourceId();
            rt.set(Role.REPRESENTATIVE, newLeaf);
            if (rt.get(Role.LEFT_RT, 0) == newLeaf) {
                // this is the first node of the new bucket
                rt.unset(Role.LEFT_RT); // disconnect from newLeaf
            }
            if (!rt.isEmpty(Role.RIGHT_RT)) {
                printText = "Node " + id + " is the " + counter +
                        "th node of ex-bucket " + oldLeaf + ". Node " +
                        newLeaf + " is its new representative. " +
                        "Forwarding request to its right neighbor with id = " +
                        rt.get(Role.RIGHT_RT, 0) + "...";
                this.print(msg, data.getInitialNode());
                // forward to next bucket node
                msg.setDestinationId(rt.get(Role.RIGHT_RT, 0));
                send(msg);
            }
            else {
                // this is the last node of the bucket. Send size response to
                // its representative and print messages.
                printText = "Node " + id + " is the last node of ex-bucket " +
                        oldLeaf + ". Node " + newLeaf +
                        " is its new representative. Routing table has been built.";
                this.print(msg, data.getInitialNode());

                // ExtendResponse exData = new ExtendResponse(counter -
                // optimalBucketSize - 1, data.getInitialNode());
                // send(new Message(id, newLeaf, exData));
                // GetSubtreeSizeRequest sizeData = new GetSubtreeSizeRequest(
                // Mode.CHECK_BALANCE, data.getInitialNode());
                // send(new Message(oldLeaf, newLeaf, sizeData));

                // disconnect the last bucket node of the old leaf
                DisconnectMessage discData = new DisconnectMessage(id,
                        Role.LAST_BUCKET_NODE, initialNode);
                send(new Message(id, oldLeaf, discData));

                ConnectMessage conn = new ConnectMessage(id,
                        Role.LAST_BUCKET_NODE, true, initialNode);
                send(new Message(id, newLeaf, conn));

                msg = new Message(id, id, new PrintMessage(msg.getType(),
                        data.getInitialNode()));

                printTree(msg);
            }
        }
    }

    void bucketNodeToLeftLeaf(Message msg) {
        ExtendRequest data = (ExtendRequest) msg.getData();
        long oldLeaf = rt.get(Role.REPRESENTATIVE);
        long rightNeighbor = rt.get(Role.RIGHT_RT, 0);

        // forward the request to the right neighbor
        msg.setSourceId(id);
        msg.setDestinationId(rightNeighbor);
        send(msg);

        // set the old leaf as the parent of this node
        rt.set(Role.PARENT, oldLeaf);
        // set the old leaf as the left adjacent node of this node
        rt.set(Role.RIGHT_A_NODE, oldLeaf);
        rt.unset(Role.REPRESENTATIVE, oldLeaf); // disconnect the representative
        // set the right neighbor as the bucketNode of this node
        rt.set(Role.FIRST_BUCKET_NODE, rightNeighbor);
        // empty routing tables
        rt.unset(Role.LEFT_RT);
        rt.unset(Role.RIGHT_RT);

        // trick, accounts for odd vs even optimal sizes
        long optimalBucketSize = (data.getOldOptimalBucketSize() - 1) / 2;
        this.storedMsgData.put(Key.BUCKET_SIZE, optimalBucketSize);

        printText = "Bucket node " + id +
                " successfully turned into a left leaf...";
        this.print(new Message(id, id, data), data.getInitialNode());
    }

    void bucketNodeToRightLeaf(Message msg) {
        ExtendRequest data = (ExtendRequest) msg.getData();
        long oldLeaf = rt.get(Role.REPRESENTATIVE);
        long rightNeighbor = rt.get(Role.RIGHT_RT, 0);
        long oldOptimalBucketSize = data.getOldOptimalBucketSize();

        // forward the request to the right neighbor
        data = new ExtendRequest(oldOptimalBucketSize, false,
                data.getInitialNode(), data.getOptimalHeight(),
                data.getCurrentHeight() + 1);
        msg.setSourceId(id);
        msg.setDestinationId(rightNeighbor);
        msg.setData(data);
        send(msg);

        // set the old leaf as the parent of this node
        rt.set(Role.PARENT, oldLeaf);
        // set the old leaf as the left adjacent node of this node
        rt.set(Role.LEFT_A_NODE, oldLeaf);
        // disconnect the representative
        rt.unset(Role.REPRESENTATIVE, oldLeaf);
        // set the right neighbor as the bucketNode of this node
        rt.set(Role.FIRST_BUCKET_NODE, rightNeighbor);
        // empty routing tables
        rt.unset(Role.LEFT_RT);
        rt.unset(Role.RIGHT_RT);

        // trick, accounts for odd vs even optimal sizes
        // long optimalBucketSize = (oldOptimalBucketSize - 1) / 2;
        // TODO
        // this.storedMsgData.put(Key.BUCKET_SIZE, oldOptimalBucketSize - 2 -
        // optimalBucketSize);

        // TODO make new routing tables
        printText = "Bucket node " + id +
                " successfully turned into a right leaf...";
        this.print(new Message(id, id, data), data.getInitialNode());
    }

    void oldLeaftoInnerNode(long lChild, long rChild, long oldLeaf,
            long initialNode) {

        // set lChild as the left child of the old leaf
        ConnectMessage connData = new ConnectMessage(lChild, Role.LEFT_CHILD,
                true, initialNode);
        send(new Message(id, oldLeaf, connData));

        // disconnect the first bucket node of the old leaf
        DisconnectMessage discData = new DisconnectMessage(lChild,
                Role.FIRST_BUCKET_NODE, initialNode);
        send(new Message(id, oldLeaf, discData));

        // set rChild as the right child of the old leaf
        connData = new ConnectMessage(rChild, Role.RIGHT_CHILD, true,
                initialNode);
        send(new Message(id, oldLeaf, connData));

        // disconnect from left neighbor as right neighbor
        discData = new DisconnectMessage(rChild, Role.RIGHT_RT, 0, initialNode);
        send(new Message(id, rt.get(Role.LEFT_RT, 0), discData));
    }

    void forwardExtendResponse(Message msg) {
        assert msg.getData() instanceof ExtendResponse;
        ExtendResponse data = (ExtendResponse) msg.getData();
        if (data.getBucketSize() != ExtendResponse.DEF_VAL) {
            finalizeExtension(data.getBucketSize());
            return;
        }

        // boolean childrenAreLeaves = rt.get(Role.LEFT_A_NODE) == rt
        // .get(Role.LEFT_CHILD) ||
        // rt.get(Role.RIGHT_A_NODE) == rt.get(Role.RIGHT_CHILD);
        // if (!this.isLeaf() || childrenAreLeaves) {
        // storedMsgData.remove(Key.BUCKET_SIZE);
        // }
        int index = data.getIndex();
        long lChild0 = data.getLeftChild();
        long rChild0 = data.getRightChild();
        if (index == 0) {
            storedMsgData.remove(Key.BUCKET_SIZE);
            // add a link from left adjacent to left child
            if (rt.contains(Role.LEFT_A_NODE) &&
                    rt.get(Role.LEFT_A_NODE) != lChild0) {
                ConnectMessage connData = new ConnectMessage(lChild0,
                        Role.RIGHT_A_NODE, true, data.getInitialNode());
                msg = new Message(id, rt.get(Role.LEFT_A_NODE), connData);
                send(msg);
            }

            // add a link from left child to left adjacent
            if (lChild0 != RoutingTable.DEF_VAL &&
                    rt.get(Role.LEFT_A_NODE) != lChild0) {
                ConnectMessage connData = new ConnectMessage(
                        rt.get(Role.LEFT_A_NODE), Role.LEFT_A_NODE, true,
                        data.getInitialNode());
                msg = new Message(id, lChild0, connData);
                send(msg);
            }

            // add a link from right adjacent to right child
            if (rt.contains(Role.RIGHT_A_NODE) &&
                    rt.get(Role.RIGHT_A_NODE) != rChild0) {
                ConnectMessage connData = new ConnectMessage(rChild0,
                        Role.LEFT_A_NODE, true, data.getInitialNode());
                msg = new Message(id, rt.get(Role.RIGHT_A_NODE), connData);
                send(msg);
            }

            // add a link from right child to right adjacent
            if (rChild0 != RoutingTable.DEF_VAL &&
                    rt.get(Role.RIGHT_A_NODE) != rChild0) {
                ConnectMessage connData = new ConnectMessage(
                        rt.get(Role.RIGHT_A_NODE), Role.RIGHT_A_NODE, true,
                        data.getInitialNode());
                msg = new Message(id, rChild0, connData);
                send(msg);
            }

            // add a link from right child to left child
            if (rChild0 != RoutingTable.DEF_VAL) {
                ConnectMessage connData = new ConnectMessage(lChild0,
                        Role.LEFT_RT, 0, true, data.getInitialNode());
                msg = new Message(id, rChild0, connData);
                send(msg);
            }

            // add a link from left child to right child
            if (lChild0 != RoutingTable.DEF_VAL) {
                ConnectMessage connData = new ConnectMessage(rChild0,
                        Role.RIGHT_RT, 0, true, data.getInitialNode());
                msg = new Message(id, lChild0, connData);
                send(msg);
            }

            rt.set(Role.LEFT_A_NODE, lChild0);
            rt.set(Role.RIGHT_A_NODE, rChild0);

            for (int i = 0; i < rt.size(Role.LEFT_RT); i++) {
                long node = rt.get(Role.LEFT_RT, i);
                data = new ExtendResponse(-i - 1, lChild0, rChild0,
                        data.getInitialNode());
                msg = new Message(msg.getSourceId(), node, data);
                send(msg);
            }
            for (int i = 0; i < rt.size(Role.RIGHT_RT); i++) {
                long node = rt.get(Role.RIGHT_RT, i);
                data = new ExtendResponse(i + 1, lChild0, rChild0,
                        data.getInitialNode());
                msg = new Message(msg.getSourceId(), node, data);
                send(msg);
            }
            // TODO forward extend requests to the new leaves if their size is
            // not optimal
            // ExtendRequest exData = new ExtendRequest(ecData.get(D2TreeCore.),
            // );
            printText = "Tree extension is over. Setting status to normal.";
            // this.print(msg, initialNode);
            this.print(msg, data.getInitialNode());
            setMode(Mode.NORMAL, data.getInitialNode());
        }
        else {
            long lChildi = rt.get(Role.LEFT_CHILD);
            long rChildi = rt.get(Role.RIGHT_CHILD);
            if (index < 0) { // old leaf's left RT
                if (index == -1) { // this is the left neighbor of the old leaf

                    if (rChildi != RoutingTable.DEF_VAL) {
                        // add a link from this node's right child to the
                        // original left child
                        ConnectMessage connData = new ConnectMessage(lChild0,
                                Role.RIGHT_RT, 0, true, data.getInitialNode());
                        msg = new Message(id, rChildi, connData);
                        send(msg);
                    }

                    if (lChild0 != RoutingTable.DEF_VAL) {
                        // add a link from the original left child to this
                        // node's right child
                        ConnectMessage connData = new ConnectMessage(rChildi,
                                Role.LEFT_RT, 0, true, data.getInitialNode());
                        msg = new Message(id, lChild0, connData);
                        send(msg);
                    }
                }

                if (lChild0 != RoutingTable.DEF_VAL) {
                    // add a link from the original left child to this node's
                    // left child
                    ConnectMessage connData = new ConnectMessage(lChildi,
                            Role.LEFT_RT, -index, true, data.getInitialNode());
                    msg = new Message(id, lChild0, connData);
                    send(msg);
                }

                if (rChild0 != RoutingTable.DEF_VAL) {
                    // add a link from the original right child to this node's
                    // right child
                    ConnectMessage connData = new ConnectMessage(rChildi,
                            Role.LEFT_RT, -index, true, data.getInitialNode());
                    msg = new Message(id, rChild0, connData);
                    send(msg);
                }

                if (lChildi != RoutingTable.DEF_VAL) {
                    // add a link from this node's left child to the original
                    // left child
                    ConnectMessage connData = new ConnectMessage(lChild0,
                            Role.RIGHT_RT, -index, true, data.getInitialNode());
                    msg = new Message(id, lChildi, connData);
                    send(msg);
                }

                if (rChildi != RoutingTable.DEF_VAL) {
                    // add a link from this node's right child to the original
                    // right child
                    ConnectMessage connData = new ConnectMessage(rChild0,
                            Role.RIGHT_RT, -index, true, data.getInitialNode());
                    msg = new Message(id, rChildi, connData);
                    send(msg);
                }
            }
            else { // old leaf's right RT
                if (index == 1) { // this is the right neighbor of the old leaf

                    if (rChildi != RoutingTable.DEF_VAL) {
                        // add a link from this node's left child to the
                        // original right child
                        ConnectMessage connData = new ConnectMessage(rChild0,
                                Role.LEFT_RT, 0, true, data.getInitialNode());
                        send(new Message(id, lChildi, connData));
                    }

                    if (rChild0 != RoutingTable.DEF_VAL) {
                        // add a link from the original right child to this
                        // node's left child
                        ConnectMessage connData = new ConnectMessage(lChildi,
                                Role.RIGHT_RT, 0, true, data.getInitialNode());
                        send(new Message(id, rChild0, connData));
                    }

                }
                if (lChildi != RoutingTable.DEF_VAL) {
                    // add a link from this node's left child to the original
                    // left child
                    ConnectMessage connData = new ConnectMessage(lChild0,
                            Role.LEFT_RT, index, true, data.getInitialNode());
                    msg = new Message(id, lChildi, connData);
                    send(msg);
                }

                if (rChildi != RoutingTable.DEF_VAL) {
                    // add a link from this node's right child to the original
                    // right child
                    ConnectMessage connData = new ConnectMessage(rChild0,
                            Role.LEFT_RT, index, true, data.getInitialNode());
                    msg = new Message(id, rChildi, connData);
                    send(msg);
                }

                if (lChild0 != RoutingTable.DEF_VAL) {
                    // add a link from the original left child to this node's
                    // left child
                    ConnectMessage connData = new ConnectMessage(lChildi,
                            Role.RIGHT_RT, index, true, data.getInitialNode());
                    msg = new Message(id, lChild0, connData);
                    send(msg);
                }

                if (rChild0 != RoutingTable.DEF_VAL) {
                    // add a link from the original right child to this node's
                    // right child
                    ConnectMessage connData = new ConnectMessage(rChildi,
                            Role.RIGHT_RT, index, true, data.getInitialNode());
                    msg = new Message(id, rChild0, connData);
                    send(msg);
                }
            }
        }
    }

    void finalizeExtension(long bucketSize) {
        boolean childrenAreLeaves = rt.get(Role.LEFT_A_NODE) == rt
                .get(Role.LEFT_CHILD) ||
                rt.get(Role.RIGHT_A_NODE) == rt.get(Role.RIGHT_CHILD);
        assert this.isLeaf() || childrenAreLeaves;
        if (this.isLeaf()) storedMsgData.put(Key.BUCKET_SIZE, bucketSize);
        if (childrenAreLeaves) storedMsgData.remove(Key.BUCKET_SIZE);
    }

    void forwardContractRequest(Message msg) {
        assert msg.getData() instanceof ContractRequest;
        // TODO contract
    }

    void forwardGetSubtreeSizeRequest(Message msg) {
        GetSubtreeSizeRequest msgData = (GetSubtreeSizeRequest) msg.getData();
        Mode msgMode = msgData.getMode();
        if (this.isBucketNode()) {
            msgData.incrementSize();
            msg.setData(msgData);
            if (rt.isEmpty(Role.RIGHT_RT)) { // node is last in its bucket
                long bucketSize = msgData.getSize();
                printText = "This node is the last in its bucket (size = " +
                        bucketSize +
                        "). Sending response to its representative, with id " +
                        rt.get(Role.REPRESENTATIVE) + ".";
                this.print(msg, msgData.getInitialNode());
                msg = new Message(id, rt.get(Role.REPRESENTATIVE),
                        new GetSubtreeSizeResponse(msgMode, bucketSize,
                                msg.getSourceId(), msgData.getInitialNode()));
                send(msg);
            }
            else {
                // printText = "Node " + this.id +
                // ". Forwarding request to its right neighbor, with id " +
                // rt.get(Role.RIGHT_RT, 0) +
                // ". (size = " + msgData.getSize() + ")";
                // this.print(msg, printText);
                long rightNeighbour = rt.get(Role.RIGHT_RT, 0);
                msg.setDestinationId(rightNeighbour);
                send(msg);
            }
        }
        else if (this.isLeaf()) {
            Long bucketSize = storedMsgData.get(Key.BUCKET_SIZE);
            // ArrayList<Long> nodes = DataExtractor.getBucketNodes(peers,
            // this);
            // assert bucketSize.intValue() == nodes.size();
            if (bucketSize == null) {
                printText = "This is a leaf. Forwarding to first bucket node with id " +
                        rt.get(Role.FIRST_BUCKET_NODE) + ".";
                this.print(msg, msgData.getInitialNode());
                msg.setDestinationId(rt.get(Role.FIRST_BUCKET_NODE));
            }
            else {
                printText = "This is a leaf with a bucket size of " +
                        bucketSize +
                        ". Forwarding response to node with id = " +
                        rt.get(Role.PARENT) + ".";
                this.print(msg, msgData.getInitialNode());
                GetSubtreeSizeResponse ssData = new GetSubtreeSizeResponse(
                        msgMode, bucketSize, msg.getSourceId(),
                        msgData.getInitialNode());
                msg = new Message(id, rt.get(Role.PARENT), ssData);
                if (!rt.contains(Role.PARENT)) msg.setDestinationId(id);
            }
            send(msg);
        }
        else {
            // printText =
            // "This is an inner node. Forwarding request to its children with ids "
            // +
            // rt.get(Role.LEFT_CHILD) +
            // " and " +
            // rt.get(Role.RIGHT_CHILD) + ".";
            // this.print(msg, msgData.getInitialNode());
            msg = new Message(msg.getSourceId(), rt.get(Role.LEFT_CHILD),
                    msg.getData());
            send(msg);
            printText = "This is an inner node. Forwarding request to child with id " +
                    msg.getDestinationId() + ".";
            this.print(msg, msgData.getInitialNode());
            // msg.setDestinationId(rt.get(Role.RIGHT_CHILD));

            // msg = new Message(msg.getSourceId(), rt.get(Role.RIGHT_CHILD),
            // msg.getData());
            // send(msg);
            // printText =
            // "This is an inner node. Forwarding request to child with id " +
            // msg.getDestinationId() + ".";
            // this.print(msg, msgData.getInitialNode());
        }
    }

    /***
     * 
     * build new data first: if the node is not a leaf, then check if info from
     * both the left and the right subtree exists else find the missing data (by
     * traversing the corresponding subtree and returning the total size of its
     * buckets)
     * 
     * 
     * then forward the message to the parent if appropriate
     * 
     * @param msg
     */
    void forwardGetSubtreeSizeResponse(Message msg) {
        assert msg.getData() instanceof GetSubtreeSizeResponse;
        GetSubtreeSizeResponse data = (GetSubtreeSizeResponse) msg.getData();
        Mode msgMode = data.getMode();
        long givenSize = data.getSize();
        long destinationID = data.getDestinationID();

        // determine what the total size of the node's subtree is
        // if this is leaf, data.getSize() gives us the size of its bucket,
        // which coincidentally is the size of its subtree
        if (!this.isLeaf()) {
            Key key = rt.get(Role.LEFT_CHILD) == msg.getSourceId() ? Key.LEFT_CHILD_SIZE
                    : Key.RIGHT_CHILD_SIZE;
            this.storedMsgData.put(key, givenSize);
            Long leftSubtreeSize = this.storedMsgData.get(Key.LEFT_CHILD_SIZE);
            Long rightSubtreeSize = this.storedMsgData
                    .get(Key.RIGHT_CHILD_SIZE);
            printText = "Incomplete subtree data (" + leftSubtreeSize + " vs " +
                    rightSubtreeSize + "). ";
            if (leftSubtreeSize == null) {
                printText += "Computing left subtree size (id = " +
                        rt.get(Role.LEFT_CHILD) + ")...";
                this.print(msg, data.getInitialNode());
                msg = new Message(destinationID, rt.get(Role.LEFT_CHILD),
                        new GetSubtreeSizeRequest(msgMode,
                                data.getInitialNode()));
                send(msg);
            }
            if (rightSubtreeSize == null) {
                printText += "Computing right subtree size (id = " +
                        rt.get(Role.RIGHT_CHILD) + ")...";
                this.print(msg, data.getInitialNode());
                msg = new Message(destinationID, rt.get(Role.RIGHT_CHILD),
                        new GetSubtreeSizeRequest(msgMode,
                                data.getInitialNode()));
                send(msg);
            }
            this.print(msg, data.getInitialNode());
            if (leftSubtreeSize == null || rightSubtreeSize == null) return;
            givenSize = leftSubtreeSize + rightSubtreeSize;
            printText = "This is not a leaf. Destination ID = " +
                    destinationID + ". (subtree size = " + givenSize +
                    ") MsgMode: " + msgMode;
        }
        else this.storedMsgData.put(Key.BUCKET_SIZE, givenSize);
        // decide if a message needs to be sent
        // if (mode == Mode.MODE_CHECK_BALANCE && (this.id == destinationID ||
        // this.isLeaf())){
        if (this.isLeaf() && this.isRoot()) return;
        if (this.id != destinationID && data.getMode() != Mode.REDISTRIBUTION) {
            if (rt.get(Role.PARENT) == destinationID) {
                printText = "Performing a balance check on parent " +
                        destinationID + "...";
                this.print(msg, data.getInitialNode());
                msg = new Message(id, rt.get(Role.PARENT),
                        new CheckBalanceRequest(givenSize,
                                data.getInitialNode()));
            }
            else {
                printText = "Node " + id + " is serving a message in " +
                        data.getMode() + ". Destination ID=" + destinationID +
                        ". Forwarding response to parent...";
                this.print(msg, data.getInitialNode());
                GetSubtreeSizeResponse ssData = new GetSubtreeSizeResponse(
                        msgMode, givenSize, data.getDestinationID(),
                        data.getInitialNode());
                msg = new Message(id, rt.get(Role.PARENT), ssData);
            }
            send(msg);
        }
        // if ((this.id == destinationID || this.isLeaf()) && mode ==
        // Mode.MODE_CHECK_BALANCE ){
        else if (this.id == destinationID &&
                data.getMode() == Mode.CHECK_BALANCE) {
            printText = "Node " + id +
                    " is in check balance mode. Destination ID=" +
                    destinationID + ". " + "Performing a balance check on " +
                    id;
            this.print(msg, data.getInitialNode());
            // CheckBalanceRequest newData = new CheckBalanceRequest(givenSize,
            CheckBalanceRequest newData = new CheckBalanceRequest(
                    data.getSize(), data.getInitialNode());
            msg.setData(newData);
            forwardCheckBalanceRequest(msg);
        }
        else if (this.isLeaf() && data.getMode() == Mode.REDISTRIBUTION) {
            long uncheckedBucketNodes = redistCore.getUncheckedBucketNodes();
            long uncheckedBuckets = redistCore.getUncheckedBucketNodes();
            long subtreeID = redistCore.getUnevenSubtreeID();
            RedistributionRequest rData = new RedistributionRequest(
                    uncheckedBucketNodes, uncheckedBuckets, subtreeID,
                    data.getInitialNode());
            msg = new Message(id, id, rData);
            forwardBucketRedistributionRequest(msg);
        }
    }

    private boolean isBalanced() {
        if (this.isLeaf()) return true;
        Long leftSubtreeSize = this.storedMsgData.get(Key.LEFT_CHILD_SIZE);
        Long rightSubtreeSize = this.storedMsgData.get(Key.RIGHT_CHILD_SIZE);
        Long totalSize = leftSubtreeSize + rightSubtreeSize;
        float nc = (float) leftSubtreeSize / (float) totalSize;
        return nc > 0.25 && nc < 0.75;
    }

    void disconnect(Message msg) {
        assert msg.getData() instanceof DisconnectMessage;
        DisconnectMessage data = (DisconnectMessage) msg.getData();
        long nodeToRemove = data.getNodeToRemove();
        int index = data.getIndex();
        RoutingTable.Role role = data.getRole();
        // if (rt.get(role, index) == nodeToRemove)
        rt.unset(role, index, nodeToRemove);
        // printText = nodeToRemove + " removed from the role of " + role +
        // " of node " + id + ".";
        // print(msg, printText, logDir + "conn-disconn.txt",
        // data.getInitialNode());
    }

    void connect(Message msg) {
        assert msg.getData() instanceof ConnectMessage;
        ConnectMessage data = (ConnectMessage) msg.getData();
        long nodeToAdd = data.getNode();
        int index = data.getIndex();
        RoutingTable.Role role = data.getRole();
        if (nodeToAdd != RoutingTable.DEF_VAL) {
            rt.set(role, index, nodeToAdd);
            // printText = nodeToAdd + " added as the " + role + " of node " +
            // id;
            // print(msg, printText, logDir + "conn-disconn.txt",
            // data.getInitialNode());
        }
        else {
            long nodeToRemove = nodeToAdd;
            rt.unset(role, index, nodeToRemove);
        }
    }

    void forwardLookupRequest(Message msg) {
        assert msg.getData() instanceof LookupRequest;
        // throw new UnsupportedOperationException("Not supported yet.");
        // System.out.println("Not supported yet.");
    }

    static void printTree(Message msg) {
        PrintMessage data = (PrintMessage) msg.getData();
        long id = msg.getDestinationId();
        if (id == msg.getSourceId()) {
            // String objectFile = PrintMessage.logDir + "routTable.bin";
            String logFile = PrintMessage.logDir + "main" +
                    data.getInitialNode() + ".txt";
            String allLogFile = PrintMessage.logDir + "main.log";
            System.out.println("Saving log to " + logFile);
            // TODO Could the removal of a peer (contract) cause problems in the
            // loop? - test this case
            for (int index = 0; index < peers.size(); index++) {
                D2TreeCore peer = peers.get(index);
                RoutingTable peerRT = peer.rt;
                PrintWriter out = null;
                try {
                    out = new PrintWriter(new FileWriter(logFile, true));
                    out.format("\nMID=%3d, Id=%3d,", msg.getMsgId(), peer.id);
                    peerRT.print(out);
                    out.close();

                    out = new PrintWriter(new FileWriter(allLogFile, true));
                    out.format("\nMID=%3d, Id=%3d,", msg.getMsgId(), peer.id);
                    peerRT.print(out);
                    out.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
            // try {
            // ObjectOutputStream oos = new ObjectOutputStream(
            // new FileOutputStream(objectFile));
            // oos.writeLong(msg.getMsgId());
            // oos.writeObject(routingTables);
            // oos.close();
            // }
            // catch (FileNotFoundException e) {
            // // TODO Auto-generated catch block
            // e.printStackTrace();
            // }
            // catch (IOException e) {
            // // TODO Auto-generated catch block
            // e.printStackTrace();
            // }
        }
        if (data.getSourceType() == D2TreeMessageT.JOIN_REQ ||
                data.getSourceType() == D2TreeMessageT.JOIN_RES) return;
        String allLogFile = PrintMessage.logDir + "main.txt";
        // String logFile = logDir + "main" + data.getInitialNode() + ".log";
        String logFile = PrintMessage.logDir + "tree.txt";
        PrintWriter out1 = null;
        PrintWriter out2 = null;
        try {
            out1 = new PrintWriter(new FileWriter(allLogFile, true));
            out2 = new PrintWriter(new FileWriter(logFile, true));
            for (int index = 0; index < peers.size(); index++) {
                D2TreeCore peer = peers.get(index);
                String msgType = peer.isRoot() ? D2TreeMessageT.toString(data
                        .getSourceType()) + "\n" : "";
                out1.format("\n%s MID=%5d, Id=%3d,", msgType, msg.getMsgId(),
                        peer.id);
                out2.format("\n%s MID=%5d, Id=%3d,", msgType, msg.getMsgId(),
                        peer.id);
                peer.rt.print(out1);
                peer.rt.print(out2);
                if (data.getSourceType() == D2TreeMessageT.PRINT_ERR_MSG &&
                        peer.id == id) {
                    out1.format(" <-- DISCREPANCY DETECTED");
                    out2.format(" <-- DISCREPANCY DETECTED");
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        out1.close();
        out2.close();
    }

    boolean isLeaf() {
        // leaves don't have children or a representative
        boolean itIs = !rt.contains(Role.REPRESENTATIVE) &&
                !rt.contains(Role.LEFT_CHILD) && !rt.contains(Role.RIGHT_CHILD);
        if (itIs && !rt.contains(Role.FIRST_BUCKET_NODE)) {
            try {
                PrintWriter out = new PrintWriter(new FileWriter(
                        PrintMessage.logDir + "isLeaf.log", true));
                new RuntimeException().printStackTrace(out);
                out.print("ID = " + id + ", ");
                rt.print(out);
                out.close();
                Thread.sleep(2000);
                return !rt.contains(Role.REPRESENTATIVE) &&
                        !rt.contains(Role.LEFT_CHILD) &&
                        !rt.contains(Role.RIGHT_CHILD);
            }
            catch (IOException | InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return !rt.contains(Role.REPRESENTATIVE) &&
                !rt.contains(Role.LEFT_CHILD) && !rt.contains(Role.RIGHT_CHILD);
    }

    boolean isBucketNode() {
        // bucketNodes have a representative
        return rt.contains(Role.REPRESENTATIVE);
    }

    boolean isRoot() {
        // the root has no parent and no representative
        return !rt.contains(Role.PARENT) && !rt.contains(Role.REPRESENTATIVE);
    }

    int getRtSize() {
        return rt.size();
    }

    void send(Message msg) {
        if (msg.getDestinationId() == msg.getSourceId())
        // throw new IllegalArgumentException();
            new IllegalArgumentException().printStackTrace();
        if (msg.getData() instanceof TransferRequest &&
                msg.getDestinationId() == id)
            new IllegalArgumentException().printStackTrace();
        if (msg.getData() instanceof ConnectMessage) {
            ConnectMessage data = (ConnectMessage) msg.getData();
            if (msg.getDestinationId() == data.getNode())
            // throw new IllegalArgumentException();
                new IllegalArgumentException().printStackTrace();
        }
        if (msg.getDestinationId() == RoutingTable.DEF_VAL) {
            NullPointerException ex = new NullPointerException();
            // findRTInconsistencies();
            ex.printStackTrace();
            System.err.println(msg);
            System.err.println(msg.getData());
            // throw ex;
            msg = new Message(id, id, new PrintMessage(
                    D2TreeMessageT.PRINT_MSG, id));
            printTree(msg);
        }
        else net.sendMsg(msg);
    }

    long getOptimalHeight(double averageBucketSize, long oldTreeHeight) {
        long treeHeight = oldTreeHeight;
        while (shouldContract(treeHeight, averageBucketSize)) {
            treeHeight = treeHeight - 1;
            averageBucketSize = 2 * averageBucketSize + 2;
        }
        while (shouldExtend(treeHeight, averageBucketSize)) {
            treeHeight = treeHeight + 1;
            averageBucketSize = (averageBucketSize - 2) / 2;
        }
        return treeHeight;
    }

    private boolean should(ECMode ecMode, long treeHeight,
            double averageBucketSize) {
        double factor = 2.0;
        double offset = 4.0;
        if (ecMode == ECMode.EXTEND) return factor * treeHeight + offset <= averageBucketSize;
        else // (ecMode == ECMode.CONTRACT)
        return treeHeight >= averageBucketSize * factor + offset;
    }

    boolean shouldExtend(long treeHeight, double averageBucketSize) {
        return should(ECMode.EXTEND, treeHeight, averageBucketSize);
    }

    boolean shouldContract(long treeHeight, double averageBucketSize) {
        return should(ECMode.CONTRACT, treeHeight, averageBucketSize);
    }

    void setMode(Mode mode, long initialNode) {
        if (this.mode == mode) return;
        String pText = "Setting mode from " + this.mode + " to " + mode;
        PrintMessage data = new PrintMessage(D2TreeMessageT.PRINT_MSG,
                initialNode);
        print(new Message(id, id, data), pText, initialNode);
        this.mode = mode;
    }

    RoutingTable getRT() {
        return this.rt;
    }

    private void print(Message msg, long initialNode) {
        print(msg, this.printText, initialNode);
        this.printText = "";
    }

    void print(Message msg, String printText, long initialNode) {
        // this.findRTInconsistencies();
        try {
            String logFile = PrintMessage.logDir + "state" + initialNode +
                    ".txt";
            String allLogFile = PrintMessage.logDir + "main.log";
            System.out.println("Saving log to " + logFile);

            String pText = "\n %s (MID = %d, NID = %d, Initial node = %d): %s %s Hops = %d";
            PrintWriter out = new PrintWriter(new FileWriter(logFile, true));

            out.format(pText, D2TreeMessageT.toString(msg.getType()),
                    msg.getMsgId(), id, initialNode, printText, mode,
                    msg.getHops());
            out.close();

            out = new PrintWriter(new FileWriter(allLogFile, true));
            out.format(pText, D2TreeMessageT.toString(msg.getType()),
                    msg.getMsgId(), id, initialNode, printText, mode,
                    msg.getHops());
            out.close();
            printText = "";
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    static void printErr(Exception ex, long initialNode) {
        ex.printStackTrace();
        try {
            String logFile = PrintMessage.logDir + "errors" + initialNode +
                    ".txt";
            // System.out.println("Saving log to " + logFile);
            PrintWriter out = new PrintWriter(new FileWriter(logFile, true));
            ex.printStackTrace(out);
            out.close();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
