package d2tree;

import java.util.ArrayList;
import java.util.Collections;

import p2p.simulator.message.Message;
import p2p.simulator.message.MessageT;
import p2p.simulator.network.Network;
import d2tree.LookupRequest.KeyPosition;
import d2tree.LookupRequest.LookupMode;
import d2tree.LookupRequest.LookupPhase;
import d2tree.RoutingTable.Role;

public class D2TreeIndexCore {
    Network             net;
    private long        id;
    ArrayList<Double>   keys;
    private int         pendingQueries;
    static final String indexLogFile = PrintMessage.indexLogDir + "lookup.txt";
    double              lVWeight;
    double              rVWeight;
    double              bVWeight;
    double              vWeight;

    // legacy data
    ArrayList<Double>   legacyKeys;
    private long        legacyHost;

    D2TreeIndexCore(long id, Network network) {
        this.net = network;
        this.id = id;
        keys = new ArrayList<Double>();
        legacyKeys = new ArrayList<Double>();
        if (id == 1) keys.add(0.0);
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
        this.legacyKeys = anotherIndexCore.legacyKeys;
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
        // // uncomment to force code not to work, comment to work it
        // if (msg != null) return;

        LookupRequest data = (LookupRequest) msg.getData();
        double key = data.getKey();

        // //uncomment when ready
        // data.updateBounds(Collections.min(keys), Collections.max(keys));

        long targetNodeId = RoutingTable.DEF_VAL;
        // long targetNodeId = nextLookupTarget(data, coreRT);
        if (keyIsInRange(key)) {
            resolveSubrequest(msg);
        }
        else if (data.getLookupMode() == LookupMode.VIA_LEAVES) {
            KeyPosition pos = KeyPosition.getPosition(key,
                    Collections.min(keys), Collections.max(keys));
            if (!coreRT.isLeaf() && !coreRT.isBucketNode()) {
                if (pos == KeyPosition.GREATER) targetNodeId = coreRT
                        .get(Role.RIGHT_A_NODE);
                else targetNodeId = coreRT.get(Role.LEFT_A_NODE);

                msg.setDestinationId(targetNodeId);
                data.setKeyPosition(pos);
                msg.setData(data);
                net.sendMsg(msg);
            }
            else if (coreRT.isLeaf()) {
                int keyDistance = data.getKeyRTDistance();
                Role role = null;
                assert pos == KeyPosition.GREATER || pos == KeyPosition.LESS;
                if (pos == KeyPosition.GREATER) role = Role.RIGHT_RT;
                else role = Role.LEFT_RT;

                if (keyDistance < 0) {
                    keyDistance = coreRT.size(role);
                    targetNodeId = coreRT.get(role, keyDistance - 1);
                }
                else if (keyDistance > 1) {
                    keyDistance--;
                    targetNodeId = coreRT.get(role, keyDistance - 1);
                }
                else {
                    assert keyDistance == 1;
                    if (pos == KeyPosition.GREATER) {
                        targetNodeId = coreRT.get(Role.LAST_BUCKET_NODE);
                        data.addToQueue(coreRT.get(Role.RIGHT_A_NODE));
                        data.addToQueue(coreRT.get(Role.RIGHT_RT, 0));
                    }
                    else {
                        assert pos == KeyPosition.LESS;
                        targetNodeId = coreRT.get(Role.LEFT_RT, 0);
                    }
                }

                msg.setDestinationId(targetNodeId);
                data.setKeyPosition(pos);
                data.setKeyRTDistance(keyDistance);
                msg.setData(data);
                net.sendMsg(msg);
            }
            else {
                assert coreRT.isBucketNode();
                if (pos == KeyPosition.LESS) {
                    if (coreRT.isEmpty(Role.LEFT_RT)) {
                        msg.setDestinationId(targetNodeId);
                        resolveSubrequest(msg, targetNodeId);
                        return;
                    }
                    else {
                        targetNodeId = coreRT.get(Role.LEFT_RT, 0);
                    }
                }
                else if (pos == KeyPosition.GREATER) {
                    if (coreRT.isEmpty(Role.RIGHT_RT)) {
                        targetNodeId = data.getNextInQueue();

                        msg.setDestinationId(targetNodeId);
                        data.setKeyPosition(pos);
                        msg.setData(data);
                        net.sendMsg(msg);
                    }
                    else {
                        resolveSubrequest(msg);
                    }
                }
            }
        }
        else {
            slowLookup(msg, key, targetNodeId);
        }
    }

    void slowLookup(Message msg, double key, long targetNodeId) {

        LookupRequest data = (LookupRequest) msg.getData();
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
        String printText = String
                .format("Forwarding lookup request for key %d to %d", key,
                        targetNodeId);
        PrintMessage.print(msg, printText, indexLogFile);
    }

    private void resolveSubrequest(Message msg) {
        resolveSubrequest(msg, msg.getSourceId());
    }

    private void resolveSubrequest(Message msg, long dest) {
        LookupRequest data = (LookupRequest) msg.getData();
        double key = data.getKey();
        this.decreasePendingQueries();
        boolean keyExists = keys.contains(key);
        if (msg.getType() == MessageT.INSERT_REQ) {
            if (!keyExists) keys.add(key);
            InsertResponse rData = new InsertResponse(key, keyExists, msg);
            net.sendMsg(new Message(id, dest, rData));
        }
        else if (msg.getType() == MessageT.DELETE_REQ) {
            if (keyExists) keys.remove(key);
            DeleteResponse rData = new DeleteResponse(key, keyExists, msg);
            net.sendMsg(new Message(id, dest, rData));
        }
        else {
            assert msg.getType() == MessageT.LOOKUP_REQ;
            LookupResponse rData = new LookupResponse(key, keyExists, msg);
            net.sendMsg(new Message(id, dest, rData));
        }
        String printText = String
                .format("Request of type %s for key %d has reached target %d after %d hops",
                        MessageT.toString(msg.getType()), key, id,
                        msg.getHops());
        PrintMessage.print(msg, printText, indexLogFile);

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
        double key = data.getKey();
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

    long nextLookupTargetOnlyLeaves(double key, RoutingTable rt, int rtDistance) {
        double minRange = Collections.min(keys);
        double maxRange = Collections.max(keys);
        Role role;
        if (key < minRange) {
            role = Role.LEFT_RT;
        }
        else if (key > maxRange) {
            role = Role.RIGHT_RT;
        }
        else return id;

        if (rtDistance < 0) {
            rtDistance = rt.size(role);
        }
        else if (rtDistance < 0) {
            rtDistance--;
        }

        long nextLookupTargetID = rt.get(role, rtDistance);
        return nextLookupTargetID;
    }

    void forwardKeyReplacementRequest(Message msg, RoutingTable rt) {
        KeyReplacementRequest data = (KeyReplacementRequest) msg.getData();
        boolean newKeysAreComing = !data.getKeys().isEmpty();
        if (data.getDestinationPeer() == id) {
            assert newKeysAreComing;
            if (newKeysAreComing) {
                this.legacyKeys = new ArrayList<Double>(keys);
                this.keys = new ArrayList<Double>(data.getKeys());
            }
        }
        else if (newKeysAreComing) {
            msg.setDestinationId(data.getDestinationPeer());
            net.sendMsg(msg);
        }
        else { // destination != id and new keys are not coming
            if (!this.keys.isEmpty() && !this.legacyKeys.isEmpty()) {
                data.setKeys(new ArrayList<Double>(legacyKeys));
                this.legacyKeys = new ArrayList<Double>();

                msg.setData(data);
                msg.setDestinationId(data.getDestinationPeer());
                net.sendMsg(msg);
            }
            else if (!this.keys.isEmpty()) {
                this.legacyKeys = new ArrayList<Double>(this.keys);
                this.keys = new ArrayList<Double>();

                data.setKeys(new ArrayList<Double>(legacyKeys));
                msg.setData(data);
                msg.setDestinationId(data.getDestinationPeer());
                net.sendMsg(msg);
            }
        }

        // if (!this.keys.isEmpty() && !this.legacyKeys.isEmpty()) {
        // // node has replaced its normal keys with new ones. Legacy is a
        // // shadow copy of the old ones because they haven't been requested
        // // yet.
        // if (!newKeysAreComing) {
        // data.setKeys(legacyKeys);
        // msg.setDestinationId(data.getDestinationPeer());
        // net.sendMsg(msg);
        // }
        // else
        // ;// DUNNO
        // }
        // else if (!this.legacyKeys.isEmpty()) {
        // // node has given its data somewhere
        // if (newKeysAreComing) {
        // this.legacyKeys = new ArrayList<Double>();
        // this.keys = new ArrayList<Double>(data.getKeys());
        // }
        // else {
        // // there is legacy data but the message has no new keys
        // }
        // }
        // else if (!this.keys.isEmpty()) {
        // // this is normal, balanced behavior
        // if (newKeysAreComing) {
        // // message disrupts the peace
        // if (data.getDestinationPeer() == id) {
        // this.legacyKeys = new ArrayList<Double>(keys);
        // this.keys = new ArrayList<Double>(data.getKeys());
        // }
        // else {
        // msg.setDestinationId(data.getDestinationPeer());
        // net.sendMsg(msg);
        // }
        // }
        // else {
        // if (data.getDestinationPeer() != id) {
        // this.legacyKeys = new ArrayList<Double>(keys);
        // msg.setDestinationId(data.getDestinationPeer());
        // net.sendMsg(msg);
        // }
        // }
        //
        // }
    }

    private boolean keyIsInRange(double key) {
        double minRange = Collections.min(keys);
        double maxRange = Collections.max(keys);
        if (key < minRange) {
            return false;
        }
        else if (key > maxRange) {
            return false;
        }
        return true;
    }

    int determineNewKeyDistance(double key, RoutingTable rt, int rtDistance) {
        double minRange = Collections.min(keys);
        double maxRange = Collections.max(keys);
        Role role;
        if (key < minRange) {
            role = Role.LEFT_RT;
        }
        else if (key > maxRange) {
            role = Role.RIGHT_RT;
        }
        else return 0;

        if (rtDistance < 0) {
            rtDistance = rt.size(role);
        }
        else {
            rtDistance--;
        }
        return rtDistance;
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
