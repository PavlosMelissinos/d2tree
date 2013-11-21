/*
 * To change this template, choose Tools | Templates and open the template in
 * the editor.
 */

package d2tree;

import p2p.simulator.message.MessageBody;

/**
 * 
 * @author Pavlos Melissinos
 */
class JoinResponse extends MessageBody {
    private static final long serialVersionUID = 5322337532327918893L;

    private long              lastBucketNode;

    JoinResponse(long lastBucketNode) {
        this.lastBucketNode = lastBucketNode;
    }

    long getLastBucketNode() {
        return this.lastBucketNode;
    }

    @Override
    public int getType() {
        return D2TreeMessageT.JOIN_RES;
    }

}
