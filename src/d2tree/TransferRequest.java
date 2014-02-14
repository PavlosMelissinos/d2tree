package d2tree;

import p2p.simulator.message.MessageBody;

public class TransferRequest extends MessageBody {

    private static final long serialVersionUID = 6271863293693936428L;
    private int               passes;
    private long              destBucket;
    private long              pivotBucket;
    private int               passIndex;
    private long              initialNode;

    public TransferRequest(long destBucket, long pivotBucket, int passes,
            long initialNode) {
        this.destBucket = destBucket;
        this.pivotBucket = pivotBucket;
        this.passes = passes;
        this.passIndex = 1;
        this.initialNode = initialNode;
    }

    public long getDestBucket() {
        return this.destBucket;
    }

    public long getPivotBucket() {
        return this.pivotBucket;
    }

    public int getPasses() {
        return this.passes;
    }

    public void incrementPassIndex() {
        this.passIndex++;
    }

    public int getPassIndex() {
        return this.passIndex;
    }

    public long getInitialNode() {
        return this.initialNode;
    }

    @Override
    public int getType() {
        // TODO Auto-generated method stub
        return D2TreeMessageT.TRANSFER_REQ;
    }

}
