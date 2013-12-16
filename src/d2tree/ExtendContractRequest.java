package d2tree;

import p2p.simulator.message.MessageBody;

public class ExtendContractRequest extends MessageBody {

    private static final long serialVersionUID = -671508466340485564L;
    private long              height;
    private long              initialNode;
    private long              totalBucketNodes;

    public ExtendContractRequest(long totalBucketNodes, long height,
            long initialNode) {
        this.height = height;
        this.initialNode = initialNode;
        this.totalBucketNodes = totalBucketNodes;
    }

    public long getInitialNode() {
        return initialNode;
    }

    public long getHeight() {
        return this.height;
    }

    public long getTotalBucketNodes() {
        return this.totalBucketNodes;
    }

    @Override
    public int getType() {
        return D2TreeMessageT.EXTEND_CONTRACT_REQ;
    }

}
