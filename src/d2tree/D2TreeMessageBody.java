package d2tree;

import p2p.simulator.message.MessageBody;

public class D2TreeMessageBody extends MessageBody {

    /**
     * 
     */
    private static final long serialVersionUID = 5122435607855200048L;
    long                      initialNode;
    int                       msgType;

    public D2TreeMessageBody(int msgType, long initialNode) {
        this.initialNode = initialNode;
        this.msgType = msgType;
    }

    @Override
    public int getType() {
        // TODO Auto-generated method stub
        return msgType;
    }

    public long getInitialNode() {
        return this.initialNode;
    }

}
