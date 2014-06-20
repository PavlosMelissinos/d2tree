package d2tree;

import java.util.TreeSet;

import p2p.simulator.message.MessageBody;

/*
 * To change this template, choose Tools | Templates and open the template in
 * the editor.
 */

public class KeyReplacementRequest extends MessageBody {
    private static final long serialVersionUID = 6809029793982822930L;
    private TreeSet<Long>     keys;
    private Mode              mode;
    private long              sourcePeer;
    private long              destinationPeer;
    private boolean           forceBucketVisit;
    private boolean           forcedInsertion;

    enum Mode {
        INORDER,
        // POSTORDER,
        // PREORDER,
        REVERSE_INORDER,
        // REVERSE_POSTORDER,
        // REVERSE_PREORDER,
        MANUAL;
    }

    // public KeyReplacementRequest(long sourcePeer) {
    // keys = new TreeSet<Double>();
    // mode = Mode.MANUAL;
    // this.sourcePeer = sourcePeer;
    // this.destinationPeer = RoutingTable.DEF_VAL;
    // this.forcedInsertion = false;
    // }

    public KeyReplacementRequest(long sourcePeer, long destinationPeer) {
        keys = new TreeSet<Long>();
        mode = Mode.MANUAL;
        this.sourcePeer = sourcePeer;
        this.destinationPeer = destinationPeer;
        this.forcedInsertion = false;
    }

    public KeyReplacementRequest(long sourcePeer, long destinationPeer,
            Mode mode, boolean forceBucketVisit) {
        keys = new TreeSet<Long>();
        this.mode = mode;
        this.sourcePeer = sourcePeer;
        this.destinationPeer = destinationPeer;
        this.forceBucketVisit = forceBucketVisit;
        this.forcedInsertion = false;
    }

    TreeSet<Long> getKeys() {
        return this.keys;
    }

    void setKeys(TreeSet<Long> keys) {
        this.keys = keys;
    }

    void setMode(Mode mode) {
        this.mode = mode;
    }

    boolean insertionIsForced() {
        return this.forcedInsertion;
    }

    Mode getMode() {
        return mode;
    }

    void reverseBucketVisits() {
        this.forceBucketVisit = !forceBucketVisit;
    }

    void forcedBucketVisits(boolean state) {
        this.forceBucketVisit = state;
    }

    boolean visitsBuckets() {
        return this.forceBucketVisit;
    }

    long getSourcePeer() {
        return this.sourcePeer;
    }

    long getDestinationPeer() {
        return this.destinationPeer;
    }

    // public KeyReplacementRequest(ArrayList<Double> keys, Mode mode) {
    // this.keys = keys;
    // this.mode = mode;
    // }

    @Override
    public int getType() {
        return D2TreeMessageT.REPLACE_KEY_REQ;
    }

    public void forcedInsertion(boolean value) {
        // TODO Auto-generated method stub
        this.forcedInsertion = value;
    }

}
