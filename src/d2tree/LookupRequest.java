package d2tree;

import java.util.LinkedList;

import p2p.simulator.message.MessageBody;

/*
 * To change this template, choose Tools | Templates and open the template in
 * the editor.
 */

public class LookupRequest extends MessageBody {
    private static final long serialVersionUID = 6809029793982822930L;
    private double            key;
    private KeyPosition       pos;
    private LookupPhase       phase;
    private LookupMode        mode;
    private int               keyRTDistance;
    private LinkedList<Long>  queue;

    // private Bound lowerBound;
    // private Bound upperBound;
    //
    // class Bound {
    // private double key;
    // private long address;
    //
    // Bound(double key, long address) {
    // this.key = key;
    // this.address = address;
    // }
    //
    // double getKey() {
    // return this.key;
    // }
    //
    // long getAddress() {
    // return this.address;
    // }
    // }

    public enum LookupMode {
        BALANCED, // this uses inner nodes as well, so it's probably
        VIA_LEAVES; // this is simpler and maybe faster
    }

    public enum KeyPosition {
        LESS,
        GREATER,
        L_ADJ,
        R_ADJ,
        NONE;
        static KeyPosition getPosition(double key, double minRange,
                double maxRange) {
            if (key < minRange) return LESS;
            else if (key > maxRange) return GREATER;
            else if (key == minRange) return L_ADJ;
            else if (key == maxRange) return R_ADJ;
            else return null;
        }

        KeyPosition invert() {
            if (this == LESS) return GREATER;
            else if (this == GREATER) return LESS;
            else if (this == R_ADJ) return L_ADJ;
            else if (this == L_ADJ) return R_ADJ;
            else return null;
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
        this.pos = KeyPosition.NONE;
        this.phase = LookupPhase.ZERO;
        this.mode = LookupMode.VIA_LEAVES;
        this.keyRTDistance = -1;
        this.queue = new LinkedList<Long>();
        // this.lowerBound = new Bound(-Double.MAX_VALUE, RoutingTable.DEF_VAL);
        // this.upperBound = new Bound(-Double.MAX_VALUE, RoutingTable.DEF_VAL);
    }

    public void setKeyRTDistance(int dist) {
        this.keyRTDistance = dist;
    }

    public void setKeyPosition(KeyPosition position) {
        this.pos = position;
    }

    public void setLookupPhase(LookupPhase phase) {
        this.phase = phase;
    }

    public void addToQueue(long nodeID) {
        queue.add(nodeID);
    }

    public int getKeyRTDistance() {
        return this.keyRTDistance;
    }

    public KeyPosition getKeyPosition() {
        return this.pos;
    }

    public LookupPhase getLookupPhase() {
        return this.phase;
    }

    public LookupMode getLookupMode() {
        return this.mode;
    }

    public long getNextInQueue() {
        int oldSize = queue.size();
        long nextInQueue = queue.poll();
        assert queue.size() < oldSize;
        return nextInQueue;
    }

    // public Bound getLowerBound() {
    // return this.lowerBound;
    // }
    //
    // public Bound getUpperBound() {
    // return this.upperBound;
    // }
    //
    // public void setLowerBound(double key, long address) {
    // this.lowerBound = new Bound(key, address);
    // }
    //
    // public void setUpperBound(double key, long address) {
    // this.upperBound = new Bound(key, address);
    // }
    //
    // public void updateBounds(double nodeMin, double nodeMax) {
    // assert nodeMin < nodeMax;
    // KeyPosition pos = KeyPosition.getPosition(key, nodeMin, nodeMax);
    // // TODO complete code
    // if (pos == KeyPosition.GREATER) {
    // if (nodeMax < this.upperBound.key) lowerBound.key = nodeMax;
    // }
    // else if (pos == KeyPosition.LESS) {
    // if (nodeMin > this.lowerBound.key) upperBound.key = nodeMin;
    // }
    // else if (nodeMax > key) {}
    // }

    public boolean hasQueue() {
        return !queue.isEmpty();
    }

    public double getKey() {
        return this.key;
    }

    @Override
    public int getType() {
        return D2TreeMessageT.LOOKUP_REQ;
    }

}
