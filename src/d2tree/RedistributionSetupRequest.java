package d2tree;

import p2p.simulator.message.MessageBody;

public class RedistributionSetupRequest extends MessageBody {

    private static final long serialVersionUID = 3137280817627914419L;
    public static long        DEF_VAL          = -1;
    private long              subtreeID;

    // the id of this subtree's root
    private long              totalBucketNodes;
    private long              totalBuckets;
    private long              pivotBucket;
    private long              bucketIndex;
    private long              initialNode;

    // public RedistributionSetupRequest(long totalBucketNodes, long
    // totalBuckets,
    // long bucketIndex, long subtreeID, long initialNode) {
    public RedistributionSetupRequest(long totalBuckets, long subtreeID,
            long initialNode) {
        this.totalBucketNodes = 0;
        this.totalBuckets = totalBuckets;
        this.bucketIndex = 1;
        this.subtreeID = subtreeID;
        this.pivotBucket = DEF_VAL;
        this.initialNode = initialNode;
    }

    // public RedistributionSetupRequest(long totalBucketNodes, long
    // totalBuckets,
    // long subtreeID, long pivotBucket, long initialNode) {
    // this.totalBucketNodes = totalBucketNodes;
    // this.totalBuckets = totalBuckets;
    // this.subtreeID = subtreeID;
    // this.pivotBucket = pivotBucket;
    // this.initialNode = initialNode;
    // }

    public long getTotalBucketNodes() {
        return totalBucketNodes;
    }

    public long getTotalBuckets() {
        return totalBuckets;
    }

    public long getSubtreeID() {
        return subtreeID;
    }

    public void update(long bucketSize) {
        this.totalBucketNodes += bucketSize;
        this.bucketIndex++;
    }

    public long getBucketIndex() {
        return bucketIndex;
    }

    public long getPivotBucket() {
        return pivotBucket;
    }

    public void setPivotBucket(long pivotBucket) {
        this.pivotBucket = pivotBucket;
    }

    public long getInitialNode() {
        return initialNode;
    }

    @Override
    public int getType() {
        return D2TreeMessageT.REDISTRIBUTE_SETUP_REQ;
    }

}
