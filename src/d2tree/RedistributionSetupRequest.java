package d2tree;

import p2p.simulator.message.MessageBody;

public class RedistributionSetupRequest extends MessageBody {

    private static final long serialVersionUID = 3137280817627914419L;
    public static long        DEF_VAL          = -1;
    private long              subtreeID;

    // the id of this subtree's root
    private long              totalUncheckedBucketNodes;
    private long              totalUncheckedBuckets;
    private long              initialNode;

    public RedistributionSetupRequest(long subtreeID, long initialNode) {
        this.subtreeID = subtreeID;
        this.initialNode = initialNode;
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

    @Override
    public int getType() {
        return D2TreeMessageT.REDISTRIBUTE_SETUP_REQ;
    }

}
