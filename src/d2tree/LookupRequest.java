package d2tree;

import p2p.simulator.message.MessageBody;

/*
 * To change this template, choose Tools | Templates and open the template in
 * the editor.
 */

public class LookupRequest extends MessageBody {
    private static final long serialVersionUID = 6809029793982822930L;
    private long              key;
    private KeyPosition       pos;
    private LookupPhase       phase;

    public enum KeyPosition {
        LESS,
        GREATER,
        NEITHER;
        static KeyPosition getPosition(double key, double minRange,
                double maxRange) {
            if (key < minRange) return LESS;
            else if (key > maxRange) return GREATER;
            else return NEITHER;
        }

        KeyPosition invert() {
            if (this == LESS) return GREATER;
            else if (this == GREATER) return LESS;
            else return NEITHER;
        }
    }

    public enum LookupPhase {
        ZERO,
        RT,
        VERTICAL,
        ADJACENT;
        LookupPhase increment() {
            if (this == ZERO) return RT;
            if (this == RT) return VERTICAL;
            else if (this == VERTICAL) return ADJACENT;
            else return ZERO;
        }
    }

    // public LookupRequest() {
    //
    // }

    public LookupRequest(long key) {
        this.key = key;
        this.pos = KeyPosition.NEITHER;
        this.phase = LookupPhase.ZERO;
    }

    public void setKeyPosition(KeyPosition position) {
        this.pos = position;
    }

    public void setLookupPhase(LookupPhase phase) {
        this.phase = phase;
    }

    public KeyPosition getKeyPosition() {
        return this.pos;
    }

    public LookupPhase getLookupPhase() {
        return this.phase;
    }

    public long getKey() {
        return this.key;
    }

    @Override
    public int getType() {
        return D2TreeMessageT.LOOKUP_REQ;
    }

}
