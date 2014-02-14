package d2tree;

import p2p.simulator.message.MessageBody;

public class TransferResponse extends MessageBody {
    private static final long serialVersionUID = 6862379902007511532L;

    private int               addedNodes;
    private long              pivotBucket;
    private long              initialNode;

    public TransferResponse(int addedNodes, long pivotBucket, long initialNode) {
        this.addedNodes = addedNodes;
        this.pivotBucket = pivotBucket;
        this.initialNode = initialNode;
    }

    public int getAddedNodes() {
        return this.addedNodes;
    }

    public long getPivotBucket() {
        return this.pivotBucket;
    }

    public long getInitialNode() {
        return initialNode;
    }

    @Override
    public int getType() {
        // TODO Auto-generated method stub
        return D2TreeMessageT.TRANSFER_RES;
    }
}
