package d2tree;

import p2p.simulator.message.Message;
import p2p.simulator.network.Network;
import d2tree.RoutingTable.Role;
import d2tree.messages.ConnectMessage;
import d2tree.messages.DisconnectMessage;
import d2tree.messages.ExtendContractRequest;
import d2tree.messages.GetSubtreeSizeRequest;
import d2tree.messages.PrintMessage;
import d2tree.messages.RedistributionRequest;
import d2tree.messages.RedistributionResponse;
import d2tree.messages.TransferRequest;
import d2tree.messages.TransferResponse;
import d2tree.messages.TransferResponse.TransferType;

public class D2TreeRedistributionCore extends D2TreeCore {

    D2TreeRedistributionCore(long id, Network net) {
        super(id, net);
    }

    /***
     * if node is leaf then get bucket size and check if any nodes need to move
     * else move to left child
     */
    // keep each bucket size at subtreesize / totalBuckets or +1 (total buckets
    // = h^2)
    void forwardBucketRedistributionRequest(D2TreeCore core, Message msg) {
        RoutingTable rt = core.rt;
        assert msg.getData() instanceof RedistributionRequest;
        RedistributionRequest data = (RedistributionRequest) msg.getData();
        // if this is an inner node, then forward to right child
        if (data.getSubtreeID() == id) {
            setMode(Mode.REDISTRIBUTION, data.getInitialNode());
        }
        if (!this.isLeaf() && !this.isBucketNode()) {
            long noofUncheckedBucketNodes = data.getNoofUncheckedBucketNodes();
            long noofUncheckedBuckets = 2 * data.getNoofUncheckedBuckets();
            long subtreeID = data.getSubtreeID();
            msg.setDestinationId(rt.get(Role.RIGHT_CHILD));
            msg.setData(new RedistributionRequest(noofUncheckedBucketNodes,
                    noofUncheckedBuckets, subtreeID, data.getInitialNode()));
            send(msg);
            printText = "Node " + id +
                    " is an inner node. Forwarding request to " +
                    msg.getDestinationId() + "(" + noofUncheckedBuckets +
                    " unchecked buckets and " + noofUncheckedBuckets +
                    " unchecked bucket nodes for subtree " + subtreeID + ").";
            this.print(msg, data.getInitialNode());

            return;
        }
        else if (this.isBucketNode())
            throw new UnsupportedOperationException("What are you doing here?");

        Long bucketSize = this.storedMsgData.get(Key.BUCKET_SIZE);
        /*
         * if it's the first time we visit the leaf, we need to prepare it for
         * what is to come, that is compute the size of its bucket and set to
         * "redistribution" mode
         */
        if (bucketSize == null) {
            printText = "Node " + id +
                    " is missing bucket info. Computing bucket size...";
            this.print(msg, data.getInitialNode());
            msg = new Message(id, rt.get(Role.FIRST_BUCKET_NODE),
                    new GetSubtreeSizeRequest(Mode.REDISTRIBUTION,
                            data.getInitialNode()));
            send(msg);
            return;
        }
        // We've reached a leaf and we've set it up, so now we need to figure
        // out which buckets to tamper with
        Long uncheckedBucketNodes = data.getNoofUncheckedBucketNodes();
        Long uncheckedBuckets = data.getNoofUncheckedBuckets();
        if (mode == Mode.CHECK_BALANCE) {
            printText = "Node " + id +
                    " is checking its balance. Terminating redistribution...";
            this.print(msg, data.getInitialNode());
            return;
        }
        // else if (mode == Mode.NORMAL) {
        else if (mode != Mode.REDISTRIBUTION && mode != Mode.TRANSFER) {
            redistData.put(Key.UNEVEN_SUBTREE_ID, data.getSubtreeID());
            redistData.put(Key.UNCHECKED_BUCKET_NODES, uncheckedBucketNodes);
            redistData.put(Key.UNCHECKED_BUCKETS, uncheckedBuckets);
            if (data.getTransferDest() != RedistributionRequest.DEF_VAL) {
                redistData.put(Key.DEST, data.getTransferDest());
            }
            else {
                setMode(Mode.REDISTRIBUTION, data.getInitialNode());
                redistData.put(Key.DEST, rt.get(Role.LEFT_RT, 0));
            }
        }

        // now that we know the size of the bucket, check if any nodes need to
        // be transferred from/to this bucket
        assert !redistData.isEmpty();
        long optimalBucketSize = uncheckedBucketNodes / uncheckedBuckets;
        long spareNodes = uncheckedBucketNodes % uncheckedBuckets;
        long diff = bucketSize - optimalBucketSize;
        long dest = redistData.containsKey(Key.DEST) ? redistData.get(Key.DEST)
                : RedistributionRequest.DEF_VAL;
        assert uncheckedBucketNodes != null;
        if (dest == id && uncheckedBucketNodes > 0) {
            if (mode == Mode.TRANSFER || diff == 0 ||
                    (diff == 1 && spareNodes > 0)) {
                // this bucket is dest and is ok, so forward request to its left
                // neighbor
                setMode(Mode.REDISTRIBUTION, data.getInitialNode());
                if (rt.isEmpty(Role.LEFT_RT)) {
                    printText = "Node " +
                            id +
                            " is dest and is ok but doesn't have any neighbors to its left." +
                            "Going back to pivot bucket with id = " +
                            msg.getSourceId() + "...";
                    this.print(msg, data.getInitialNode());
                    redistData.remove(Key.DEST);
                    msg = new Message(id, msg.getSourceId(), msg.getData());
                    send(msg);
                    return;
                }
                long newDest = rt.get(Role.LEFT_RT, 0);
                data.setTransferDest(newDest);
                msg.setDestinationId(newDest);
                msg.setData(data);
                printText = "Node " +
                        id +
                        " is dest. Its bucket is ok (size = " +
                        bucketSize +
                        "). Forwarding redistribution request to left neighbor with id = " +
                        rt.get(Role.LEFT_RT, 0) +
                        " which is the new dest bucket...";
                this.print(msg, data.getInitialNode());
            }
            else { // this bucket is dest and not ok, so send a response to the
                   // source of this message
                printText = "Node " + id +
                        " is dest. Its bucket is larger than optimal by " +
                        diff + "(" + bucketSize + " vs " + optimalBucketSize +
                        "). Sending redistribution response to node " +
                        msg.getSourceId() + "...";
                this.print(msg, data.getInitialNode());
                assert this.redistData.containsKey(Key.UNCHECKED_BUCKET_NODES);
                msg = new Message(id, msg.getSourceId(),
                        new RedistributionResponse(
                                rt.get(Role.LAST_BUCKET_NODE), bucketSize,
                                data.getInitialNode()));
                setMode(Mode.TRANSFER, data.getInitialNode());
            }
            send(msg);
        }
        else if (diff == 0 || (diff == 1 && spareNodes > 0)) {
            // this bucket is ok, so move to the next one (if there is one)
            long subtreeID = redistData.get(Key.UNEVEN_SUBTREE_ID);
            this.redistData.clear();
            setMode(Mode.NORMAL, data.getInitialNode());
            uncheckedBuckets--;
            uncheckedBucketNodes -= bucketSize;
            if (uncheckedBuckets == 0) {
                // redistribution is over so check if
                // the tree needs extension/contraction
                printText = "The tree is balanced. Doing an extend/contract test...";
                this.print(msg, data.getInitialNode());
                long totalBuckets = new Double(Math.pow(2, rt.getHeight() - 1))
                        .longValue();
                long totalBucketNodes = diff == 0 ? optimalBucketSize *
                        totalBuckets : (optimalBucketSize - 1) * totalBuckets;
                ExtendContractRequest ecData = new ExtendContractRequest(
                        totalBucketNodes, rt.getHeight(), data.getInitialNode());
                msg = new Message(id, subtreeID, ecData);
                send(msg);
            }
            else if (uncheckedBucketNodes == 0 || rt.isEmpty(Role.LEFT_RT)) {
                String leftNeighbor = rt.isEmpty(Role.LEFT_RT) ? "None"
                        : String.valueOf(rt.get(Role.LEFT_RT, 0));
                printText = "Something went wrong. Unchecked buckets: " +
                        uncheckedBuckets + ", Unchecked Bucket Nodes: " +
                        uncheckedBucketNodes + ", left neighbor: " +
                        leftNeighbor;
                this.print(msg, data.getInitialNode());
            }
            else {
                long leftNeighbor = rt.get(Role.LEFT_RT, 0);
                printText = "This bucket has the right size. Forwarding request to bucket " +
                        leftNeighbor;
                RedistributionRequest msgData = new RedistributionRequest(
                        uncheckedBucketNodes, uncheckedBuckets, subtreeID,
                        data.getInitialNode());
                if (dest == leftNeighbor)
                    msgData.setTransferDest(RedistributionRequest.DEF_VAL);

                this.print(msg, data.getInitialNode());
                msg = new Message(id, leftNeighbor, msgData);
                send(msg);
            }
        }
        else if (dest != id && rt.isEmpty(Role.LEFT_RT)) {
            long subtreeID = redistData.get(Key.UNEVEN_SUBTREE_ID);
            this.redistData.clear();
            setMode(Mode.NORMAL, data.getInitialNode());
            uncheckedBuckets--;
            uncheckedBucketNodes -= bucketSize;
            if (uncheckedBuckets == 0) {
                // redistribution is over so check if
                // the tree needs extension/contraction
                printText = "The tree is balanced enough. Doing an extend/contract test...";
                this.print(msg, data.getInitialNode());
                long totalBuckets = new Double(Math.pow(2, rt.getHeight() - 1))
                        .longValue();
                long totalBucketNodes = diff == 0 ? optimalBucketSize *
                        totalBuckets : (optimalBucketSize - 1) * totalBuckets;
                ExtendContractRequest ecData = new ExtendContractRequest(
                        totalBucketNodes, rt.getHeight(), data.getInitialNode());
                msg = new Message(id, subtreeID, ecData);
                send(msg);
            }
        }
        else {
            // nodes need to be transferred from/to this node
            // storedMsgData.put(MODE, MODE_TRANSFER);
            printText = "The bucket of node " + id +
                    " is larger than optimal by " + diff +
                    ". Forwarding request to dest = " + dest + ".";
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
                else redistData.put(Key.DEST, rt.get(Role.LEFT_RT, 0));
                dest = redistData.get(Key.DEST);
            }
            data.setTransferDest(dest);
            msg = new Message(id, dest, data);
            send(msg);
        }
    }

    void forwardBucketRedistributionResponse(Message msg) {
        assert msg.getData() instanceof RedistributionResponse;
        long destBucket = msg.getSourceId();
        long uncheckedBucketNodes = this.redistData
                .get(Key.UNCHECKED_BUCKET_NODES);
        long uncheckedBuckets = this.redistData.get(Key.UNCHECKED_BUCKETS);
        long optimalBucketSize = uncheckedBucketNodes / uncheckedBuckets;
        long bucketSize = this.storedMsgData.get(Key.BUCKET_SIZE);
        long diff = bucketSize - optimalBucketSize;
        RedistributionResponse data = (RedistributionResponse) msg.getData();
        long destDiff = data.getDestSize() - optimalBucketSize;
        if (diff * destDiff >= 0) {
            // both this bucket and dest have either more or less nodes (or
            // exactly the number we need)
            long subtreeID = this.redistData.get(Key.UNEVEN_SUBTREE_ID);
            printText = "Node " + id + " and destnode " + destBucket +
                    " both have ";
            if (diff == 0 && destDiff == 0) printText += "the right number of nodes " +
                    "(" + optimalBucketSize + "). We're done here.";
            else printText += "too large (or too small) buckets" + "(" +
                    bucketSize + " vs " + data.getDestSize() + " nodes).";

            if (diff == 0) {
                // pivot bucket is ok, so move to its left neighbor
                redistData.clear();
                RedistributionRequest rData = new RedistributionRequest(
                        uncheckedBucketNodes - bucketSize,
                        uncheckedBuckets - 1, subtreeID, data.getInitialNode());
                msg = new Message(id, rt.get(Role.LEFT_RT, 0), rData);
                printText += "Node " +
                        id +
                        "is ok. Sending a redistribution request to new pivot bucket " +
                        msg.getDestinationId() + ".";
                this.print(msg, data.getInitialNode());
                send(msg);
                setMode(Mode.NORMAL, data.getInitialNode());
            }
            else {
                RedistributionRequest rData = new RedistributionRequest(
                        uncheckedBucketNodes, uncheckedBuckets, subtreeID,
                        data.getInitialNode());
                rData.setTransferDest(destBucket);
                msg = new Message(id, destBucket, rData);

                this.print(msg, data.getInitialNode());
                send(msg);
            }
        }
        else {
            TransferRequest transfData = new TransferRequest(destBucket, id,
                    true, data.getInitialNode());
            if (diff > destDiff) { // |pivotBucket| > |destBucket|
                // move nodes from pivotBucket to destBucket
                printText = "Node " + id +
                        " has extra nodes that dest bucket " + destBucket +
                        " can use (" + bucketSize + " vs " +
                        data.getDestSize() + ")" +
                        " Performing transfer from bucket " + id +
                        " to bucket " + destBucket + ".";
                this.print(msg, data.getInitialNode());
                // msg = new Message(id, rt.get(Role.FIRST_BUCKET_NODE),
                // transfData);
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
            // move this node from the dest bucket to pivot
            long pivotNode = msg.getSourceId();
            long destNode = id;
            printText = String.format(printText, destBucket, pivotBucket);
            this.print(msg, transfData.getInitialNode());

            // remove the link from dest node's left neighbor to dest node
            DisconnectMessage discData = new DisconnectMessage(id,
                    Role.RIGHT_RT, 0, transfData.getInitialNode());
            msg = new Message(id, rt.get(Role.LEFT_RT, 0), discData);
            send(msg);

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

            TransferResponse respData = new TransferResponse(
                    TransferType.NODE_REMOVED, pivotBucket,
                    transfData.getInitialNode());
            msg = new Message(id, destBucket, respData);
            send(msg);

            respData = new TransferResponse(TransferType.NODE_ADDED,
                    pivotBucket, transfData.getInitialNode());
            msg = new Message(id, pivotBucket, respData);
            send(msg);

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
            msg = new Message(id, rt.get(Role.RIGHT_RT, 0),
                    new DisconnectMessage(id, Role.LEFT_RT, 0,
                            transfData.getInitialNode()));
            send(msg);

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
            msg = new Message(id, pivotBucket, respData);
            send(msg);

            respData = new TransferResponse(TransferType.NODE_ADDED,
                    pivotBucket, transfData.getInitialNode());
            msg = new Message(id, destBucket, respData);
            send(msg);

            printText = "Successfully moved " + pivotNode + " next to " +
                    destNode + "...";
        }
        printText += " Transfer has been successful. THE END";
        // findRTInconsistencies(true);
        print(msg, transfData.getInitialNode());
        PrintMessage printData = new PrintMessage(msg.getType(),
                transfData.getInitialNode());
        // printTree(new Message(id, rt.get(Role.REPRESENTATIVE),
        // printData));
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

            Long destBucket = redistData.get(Key.DEST);
            boolean isDestBucket = destBucket != null && destBucket == id;
            if (isDestBucket) {
                printText = String
                        .format("Node %d is dest bucket. "
                                + "Forwarding redistribution response to pivot bucket with id = %d...",
                                id, pivotBucket);
                print(msg, data.getInitialNode());
                RedistributionResponse rData = new RedistributionResponse(
                        rt.get(Role.LAST_BUCKET_NODE), bucketSize,
                        data.getInitialNode());
                msg = new Message(id, pivotBucket, rData);
                send(msg);
            }
        }
    }

}
