package d2tree;

import p2p.simulator.message.MessageBody;

public class RedistributionRequest extends MessageBody {

    private static final long serialVersionUID = 3137280817627914419L;
    public static long        DEF_VAL          = -1;
    private long              subtreeID;
    // the id of this subtree's root
    private long              totalUncheckedBucketNodes;
    private long              totalUncheckedBuckets;
    private long              transferDest;
    private long              destOffset;
    private long              initialNode;
    private long              pivotDiff;

    public RedistributionRequest(long totalUncheckedBucketNodes,
            long totalUncheckedBuckets, long subtreeID, long initialNode) {
        this.totalUncheckedBucketNodes = totalUncheckedBucketNodes;
        this.totalUncheckedBuckets = totalUncheckedBuckets;
        this.subtreeID = subtreeID;
        transferDest = DEF_VAL;
        this.initialNode = initialNode;
        destOffset = 0;
        pivotDiff = 0;
    }

    public long getTotalUncheckedBucketNodes() {
        return totalUncheckedBucketNodes;
    }

    public long getTotalUncheckedBuckets() {
        return totalUncheckedBuckets;
    }

    public long getSubtreeID() {
        return subtreeID;
    }

    public long getInitialNode() {
        return initialNode;
    }

    public long getTransferDest() {
        return transferDest;
    }

    public long getPivotDiff() {
        return pivotDiff;
    }

    public void setTransferDest(long dest) {
        this.transferDest = dest;
        this.destOffset++;
    }

    public void setDestOffset(long destOffset) {
        this.destOffset = destOffset;
    }

    public void setPivotDiff(long diff) {
        this.pivotDiff = diff;
    }

    public long getDestIndex() {
        return totalUncheckedBuckets - this.destOffset;
    }

    public long getDestOffset() {
        return this.destOffset;
    }

    @Override
    public int getType() {
        return D2TreeMessageT.REDISTRIBUTE_REQ;
    }

}
