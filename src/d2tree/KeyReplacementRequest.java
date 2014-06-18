package d2tree;

import java.util.ArrayList;

import p2p.simulator.message.MessageBody;

/*
 * To change this template, choose Tools | Templates and open the template in
 * the editor.
 */

public class KeyReplacementRequest extends MessageBody {
    private static final long serialVersionUID = 6809029793982822930L;
    private ArrayList<Double> keys;
    private Mode              mode;
    private long              sourcePeer;
    private long              destinationPeer;

    enum Mode {
        INORDER,
        // POSTORDER,
        // PREORDER,
        // REVERSE_INORDER,
        // REVERSE_POSTORDER,
        // REVERSE_PREORDER,
        MANUAL;
    }

    public KeyReplacementRequest(long sourcePeer) {
        keys = new ArrayList<Double>();
        mode = Mode.MANUAL;
        this.sourcePeer = sourcePeer;
    }

    public KeyReplacementRequest(long sourcePeer, long destinationPeer) {
        keys = new ArrayList<Double>();
        mode = Mode.MANUAL;
        this.sourcePeer = sourcePeer;
        this.destinationPeer = destinationPeer;
    }

    ArrayList<Double> getKeys() {
        return this.keys;
    }

    void setKeys(ArrayList<Double> keys) {
        this.keys = keys;
    }

    Mode getMode() {
        return mode;
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

}
