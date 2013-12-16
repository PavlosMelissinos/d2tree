package d2tree;

import p2p.simulator.message.MessageBody;

public class ExtendResponse extends MessageBody {
    private static final long serialVersionUID = -737297975099005858L;
    public static Long        DEF_VAL          = -1L;

    private int               index;
    private long              leftChild;
    private long              rightChild;
    private long              initialNode;
    private long              bucketSize;

    public ExtendResponse(int index, long leftChild, long rightChild,
            long initialNode) {
        this.index = index;
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.initialNode = initialNode;
        this.bucketSize = DEF_VAL;
    }

    public ExtendResponse(long bucketSize, long initialNode) {
        this.initialNode = initialNode;
        this.bucketSize = bucketSize;
    }

    public long getInitialNode() {
        return initialNode;
    }

    public int getIndex() {
        return this.index;
    }

    public long getBucketSize() {
        return this.bucketSize;
    }

    /***
     * 
     * @return the original left leaf
     */
    public long getLeftChild() {
        return this.leftChild;
    }

    /***
     * 
     * @return the original right leaf
     */
    public long getRightChild() {
        return this.rightChild;
    }

    @Override
    public int getType() {
        return D2TreeMessageT.EXTEND_RES;
    }

}
