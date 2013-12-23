package d2tree;

public class RedistributionCore {
    // private HashMap<Key, Long> values;
    private long        uncheckedBucketNodes;
    private long        uncheckedBuckets;
    private long        unevenSubtreeID;
    private long        surplus;
    private long        dest;
    private static long DEF_VAL = -1;

    public static enum Key {
        UNEVEN_SUBTREE_ID,
        UNCHECKED_BUCKET_NODES,
        UNCHECKED_BUCKETS,
        DEST,
        SURPLUS;
    }

    public RedistributionCore() {
        this.uncheckedBucketNodes = 0;
        this.uncheckedBuckets = 0;
        this.unevenSubtreeID = DEF_VAL;
        this.surplus = 0;
    }

    public RedistributionCore(long uncheckedBucketNodes, long uncheckedBuckets,
            long unevenSubtreeID) {
        this.uncheckedBucketNodes = uncheckedBucketNodes;
        this.uncheckedBuckets = uncheckedBuckets;
        this.unevenSubtreeID = unevenSubtreeID;
        this.surplus = 0;
    }

    public long getUncheckedBucketNodes() {
        return this.uncheckedBucketNodes;
    }

    public long getUncheckedBuckets() {
        return this.uncheckedBuckets;
    }

    public long getUnevenSubtreeID() {
        return this.unevenSubtreeID;
    }

    public long getSurplus() {
        return surplus;
    }

    public long getDest() {
        return dest;
    }

    public void setUncheckedBucketNodes(long uncheckedBucketNodes) {
        this.uncheckedBucketNodes = uncheckedBucketNodes;
    }

    // public void setUncheckedBuckets(long uncheckedBuckets) {
    // this.uncheckedBuckets = uncheckedBuckets;
    // }
    //
    // public void setUnevenSubtreeID(long unevenSubtreeID) {
    // this.unevenSubtreeID = unevenSubtreeID;
    // }
    //
    // public void setSurplus(int surplus) {
    // this.surplus = surplus;
    // }
    //
    public void setDest(long dest) {
        this.dest = dest;
    }

    public void set(RedistributionRequest data) {
        this.uncheckedBucketNodes = data.getTotalUncheckedBucketNodes() +
                this.getSurplus();
        this.uncheckedBuckets = data.getTotalUncheckedBuckets();
        this.unevenSubtreeID = data.getSubtreeID();
        this.surplus = 0;
    }

    public void increaseSurplus() {
        this.surplus++;
    }

    public void decreaseSurplus() {
        this.surplus--;
    }

    public void clear() {
        this.surplus = 0;
        this.unevenSubtreeID = DEF_VAL;
        this.dest = DEF_VAL;
        this.uncheckedBucketNodes = 0;
        this.uncheckedBuckets = 0;
    }

    public boolean isEmpty() {
        return unevenSubtreeID == DEF_VAL;
    }
}
