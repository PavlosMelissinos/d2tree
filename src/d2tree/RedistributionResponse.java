package d2tree;

import p2p.simulator.message.MessageBody;

public class RedistributionResponse extends MessageBody {

    private static final long serialVersionUID = -1704533667370020587L;
    private long              destNode;
    private long              destSize;
    private long              initialNode;

    public RedistributionResponse(long destNode, long destSize, long initialNode) {
        this.destNode = destNode;
        this.destSize = destSize;
        this.initialNode = initialNode;
    }

    public long getDestNode() {
        return this.destNode;
    }

    public long getDestSize() {
        return this.destSize;
    }

    public long getInitialNode() {
        return initialNode;
    }

    @Override
    public int getType() {
        // TODO Auto-generated method stub
        return D2TreeMessageT.REDISTRIBUTE_RES;
    }

}
