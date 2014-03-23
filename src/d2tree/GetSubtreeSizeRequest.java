package d2tree;

import p2p.simulator.message.MessageBody;
import d2tree.D2TreeCore.Mode;

public class GetSubtreeSizeRequest extends MessageBody {
    private static final long serialVersionUID = -5767816851951973009L;
    private Mode              mode;
    private long              size;
    private long              initialNode;
    private boolean           trueWeight;

    public GetSubtreeSizeRequest(Mode mode, long initialNode) {
        this.mode = mode;
        this.size = 0;
        this.initialNode = initialNode;
        this.trueWeight = false;
    }

    public GetSubtreeSizeRequest(Mode mode, long initialNode, boolean trueWeight) {
        this.mode = mode;
        this.size = 0;
        this.initialNode = initialNode;
        this.trueWeight = trueWeight;
    }

    public long getInitialNode() {
        return initialNode;
    }

    public Mode getMode() {
        return this.mode;
    }

    public boolean recomputesWeights() {
        return this.trueWeight;
    }

    public long getSize() {
        return this.size;
    }

    public void incrementSize() {
        this.size++;
    }

    @Override
    public int getType() {
        return D2TreeMessageT.GET_SUBTREE_SIZE_REQ;
    }

}
