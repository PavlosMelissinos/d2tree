package d2tree;

import java.util.ArrayList;
import java.util.Collections;

import p2p.simulator.message.Message;
import p2p.simulator.message.MessageT;
import p2p.simulator.network.Network;
import d2tree.LookupRequest.KeyPosition;
import d2tree.LookupRequest.LookupPhase;
import d2tree.RoutingTable.Role;

public class D2TreeIndexCore {
    Network             net;
    private long        id;
    ArrayList<Double>   keys;
    private int         pendingQueries;
    static final String logIndexFile = PrintMessage.logIndexDir + "lookup.txt";
    double              lVWeight;
    double              rVWeight;
    double              bVWeight;
    double              vWeight;

    D2TreeIndexCore(long id, Network network) {
        this.net = network;
        this.id = id;
        keys = new ArrayList<Double>();
        this.pendingQueries = 0;
        this.lVWeight = 0;
        this.rVWeight = 0;
        this.bVWeight = 0;
        this.vWeight = 0;
    }

    D2TreeIndexCore(D2TreeIndexCore anotherIndexCore) {
        this.id = anotherIndexCore.id;
        this.net = anotherIndexCore.net;
        this.bVWeight = anotherIndexCore.bVWeight;
        this.keys = anotherIndexCore.keys;
        this.pendingQueries = anotherIndexCore.pendingQueries;
        this.lVWeight = anotherIndexCore.lVWeight;
        this.rVWeight = anotherIndexCore.rVWeight;
        this.bVWeight = anotherIndexCore.bVWeight;
        this.vWeight = anotherIndexCore.vWeight;
    }

    void setID(long id) {
        this.id = id;
    }

    void lookup(Message msg, RoutingTable coreRT) {
        if (msg != null) return; // comment to force code not to work,
                                 // uncomment to work it
        LookupRequest data = (LookupRequest) msg.getData();
        double key = data.getKey();
        long targetNodeId = nextLookupTarget(data, coreRT);
        if (id == targetNodeId) {
            this.decreasePendingQueries();
            boolean keyExists = keys.contains(key);
            if (msg.getType() == MessageT.INSERT_REQ) {
                if (!keyExists) keys.add(key);
                InsertResponse rData = new InsertResponse(key, !keyExists, msg);
                net.sendMsg(new Message(id, msg.getSourceId(), rData));
            }
            else if (msg.getType() == MessageT.DELETE_REQ) {
                if (keyExists) keys.remove(key);
                DeleteResponse rData = new DeleteResponse(key, keyExists, msg);
                net.sendMsg(new Message(id, msg.getSourceId(), rData));
            }
            else {
                assert msg.getType() == MessageT.LOOKUP_REQ;
                LookupResponse rData = new LookupResponse(key, keyExists, msg);
                net.sendMsg(new Message(id, msg.getSourceId(), rData));
            }
            String printText = String
                    .format("Request of type %s for key %d has reached target %d after %d hops",
                            MessageT.toString(msg.getType()), key,
                            targetNodeId, msg.getHops());
            PrintMessage.print(msg, printText, logIndexFile);
        }
        else {
            LookupPhase phase = data.getLookupPhase();
            KeyPosition pos = data.getKeyPosition();
            switch (phase) {
            case ZERO:
                data.setLookupPhase(phase.increment());
                break;
            case RT:
                // TODO this will change when full routing tables are employed
                if (directionHasChanged(key, pos))
                    data.setLookupPhase(phase.increment()); // set to vertical
                break;
            case VERTICAL:
                // TODO this will change when full routing tables are employed
                if (directionHasChanged(key, pos)) {
                    data.setLookupPhase(phase.increment()); // set to adjacent
                }
                break;
            case ADJACENT: // do nothing
                break;
            default: // do nothing
                break;
            }
            if (directionHasChanged(key, pos)) pos.invert();
            data.setKeyPosition(pos);
            msg.setData(data);
            msg.setDestinationId(targetNodeId);
            net.sendMsg(msg);
            String printText = String.format(
                    "Forwarding lookup request for key %d to %d", key,
                    targetNodeId);
            PrintMessage.print(msg, printText, logIndexFile);
        }
    }

    void addValue(double value) {
        this.keys.add(value);
    }

    boolean directionHasChanged(double key, KeyPosition pos) {
        double minRange = Collections.min(keys);
        double maxRange = Collections.max(keys);
        KeyPosition newPos = KeyPosition.getPosition(key, minRange, maxRange);
        return newPos != pos;
    }

    // initially KeyPosition is NEITHER, lookup phase is FIRST
    long nextLookupTarget(LookupRequest data, RoutingTable coreRT) {
        long key = data.getKey();
        KeyPosition pos = data.getKeyPosition();
        LookupPhase phase = data.getLookupPhase();
        if (keys.isEmpty()) return id;
        double minRange = Collections.min(keys);
        double maxRange = Collections.max(keys);

        ArrayList<Long> possibleTargets = new ArrayList<Long>();
        if (key < minRange) {
            possibleTargets.add(coreRT.get(Role.LEFT_RT, 0));
            possibleTargets.add(coreRT.get(Role.LEFT_CHILD));
            possibleTargets.add(coreRT.get(Role.LEFT_A_NODE));
        }
        else if (key > maxRange) {
            possibleTargets.add(coreRT.get(Role.RIGHT_RT, 0));
            possibleTargets.add(coreRT.get(Role.RIGHT_CHILD));
            possibleTargets.add(coreRT.get(Role.RIGHT_A_NODE));
        }
        else {
            return id;
        }
        long nextTarget = id;

        switch (phase) {
        case ZERO:
            // TODO set phase to RT
            nextTarget = possibleTargets.get(0);
            break;
        case RT:
            if (directionHasChanged(key, pos)) {
                if (coreRT.isLeaf()) {
                    nextTarget = coreRT.get(Role.PARENT);
                }
                else {
                    nextTarget = possibleTargets.get(1);
                }
            }
            else {
                nextTarget = possibleTargets.get(0);
            }
            break;
        case VERTICAL:
            if (directionHasChanged(key, pos)) {
                if (coreRT.isLeaf()) {
                    // TODO send sourceSet size
                    nextTarget = possibleTargets.get(2);
                }
                else {
                    nextTarget = possibleTargets.get(1);
                }
            }
            else {
                nextTarget = possibleTargets.get(0);
            }
            break;
        case ADJACENT:
            if (directionHasChanged(key, pos)) {
                nextTarget = id;
            }
            else {
                nextTarget = possibleTargets.get(2);
            }
            break;
        default:
            throw new IllegalArgumentException(String.format(
                    "Phase % not allowed", phase.toString()));
        }

        // if our
        if (nextTarget == RoutingTable.DEF_VAL) {
            data.setLookupPhase(phase.increment());
            return nextLookupTarget(data, coreRT);
        }
        else return nextTarget;
    }

    boolean isBalanced(long height) {
        return isBalanced(height, this.lVWeight, this.rVWeight);
    }

    boolean isBalanced(long height, double lVW, double rVW) {
        double epsilonH = 1 / (Math.sqrt(height));
        double minVW = keys.size() + (1 - epsilonH) * (lVW + rVW);
        double maxVW = keys.size() + (1 + epsilonH) * (lVW + rVW);
        // double vW = getVirtualWeight(lVW, rVW);
        return vWeight > minVW && vWeight < maxVW;
    }

    void forwardVWUpdateRequest(Message msg, RoutingTable rt) {
        assert !rt.isBucketNode();
        VWUpdateRequest data = (VWUpdateRequest) msg.getData();

        double vW = data.getVirtualWeight();
        long sourceId = msg.getSourceId();

        if (rt.isLeaf()) {
            this.bVWeight = vW;
        }
        else {
            if (sourceId == rt.get(Role.LEFT_CHILD)) {
                this.lVWeight = vW;
            }
            else if (sourceId == rt.get(Role.RIGHT_CHILD)) {
                this.rVWeight = vW;
            }
        }
        double newVW = lVWeight + rVWeight + bVWeight + keys.size();
        data.setVirtualWeight(newVW);
        // if (!isBalanced(msg.getHops())) {
        if (rt.isLeaf() || !isBalanced(data.getHeight() - rt.getDepth())) {
            long destId = rt.get(Role.PARENT);
            msg.setData(data);
            msg.setDestinationId(destId);
            net.sendMsg(msg);
        }
        else {
            long unevenSubtree = sourceId;
            VWUpdateResponse rData = new VWUpdateResponse(unevenSubtree);
            net.sendMsg(new Message(id, data.getOriginalNode(), rData));
        }
    }

    long nextLookupTargetOnlyLeaves(long key, RoutingTable rt, int distance) {

        double minRange = Collections.min(keys);
        double maxRange = Collections.max(keys);

        ArrayList<Long> possibleTargets = new ArrayList<Long>();
        int rtLimit = log(distance, 2);
        if (key < minRange) {
            for (int i = rtLimit; i >= 0; i--) {
                possibleTargets.add(rt.get(Role.LEFT_RT, i));
            }
            possibleTargets.add(rt.get(Role.LEFT_A_NODE));
        }
        else if (key > maxRange) {
            for (int i = rtLimit; i >= 0; i--)
                possibleTargets.add(rt.get(Role.RIGHT_RT, i));
            possibleTargets.add(rt.get(Role.RIGHT_A_NODE));
        }
        Double difference = Math.pow(2, rtLimit);
        distance -= difference.intValue();
        // TODO continue implementation
        return 0;
    }

    void increasePendingQueries() {
        this.pendingQueries++;
    }

    void decreasePendingQueries() {
        this.pendingQueries--;
    }

    int getPendingQueries() {
        return this.pendingQueries;
    }

    private int log(int num, int base) {
        Double result = Math.log(num) / Math.log(base);
        return result.intValue();
    }
}
