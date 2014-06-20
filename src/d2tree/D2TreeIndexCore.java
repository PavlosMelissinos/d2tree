package d2tree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeSet;

import p2p.simulator.message.DeleteResponse;
import p2p.simulator.message.InsertResponse;
import p2p.simulator.message.LookupResponse;
import p2p.simulator.message.Message;
import p2p.simulator.message.MessageT;
import p2p.simulator.network.Network;
import d2tree.KeyReplacementRequest.Mode;
import d2tree.LookupRequest.KeyPosition;
import d2tree.LookupRequest.LookupMode;
import d2tree.LookupRequest.LookupPhase;
import d2tree.RoutingTable.Role;

public class D2TreeIndexCore {
    Network             net;
    private long        id;
    TreeSet<Long>       keys;
    private int         pendingQueries;
    // static final String indexLogFile = PrintMessage.indexLogDir +
    // "lookup.txt";
    static final String indexLogFile = "lookup.txt";
    double              lVWeight;
    double              rVWeight;
    double              bVWeight;
    double              vWeight;

    // legacy data
    TreeSet<Long>       legacyKeys;
    private long        legacyHost;

    static long         MIN_VALUE    = -1000000;    // -Long.MAX_VALUE;
    static long         MAX_VALUE    = 1000000;     // Long.MAX_VALUE;

    D2TreeIndexCore(long id, Network network) {
        this.net = network;
        this.id = id;
        keys = new TreeSet<Long>();
        if (id == 1) keys.add(0L);
        legacyKeys = new TreeSet<Long>();
        // legacyKeys.add();
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
        long key = data.getKey();

        // //uncomment when ready
        // data.updateBounds(Collections.min(keys), Collections.max(keys));

        long targetNodeId = RoutingTable.DEF_VAL;
        // long targetNodeId = nextLookupTarget(data, coreRT);
        if (keyIsInRange(key)) {
            resolveSubrequest(msg);
        }
        else if (data.getLookupMode() == LookupMode.VIA_LEAVES) {
            KeyPosition pos = KeyPosition.GREATER;
            if (!keys.isEmpty()) {
                pos = KeyPosition.getPosition(key, Collections.min(keys),
                        Collections.max(keys));
            }
            else if (coreRT.isEmpty(Role.RIGHT_RT)) {
                pos = KeyPosition.LESS;
            }

            if (!coreRT.isLeaf() && !coreRT.isBucketNode()) {
                if (pos == KeyPosition.GREATER) targetNodeId = coreRT
                        .get(Role.RIGHT_A_NODE);
                else targetNodeId = coreRT.get(Role.LEFT_A_NODE);

                msg.setDestinationId(targetNodeId);
                data.setKeyPosition(pos);
                msg.setData(data);
                send(msg);
            }
            else if (coreRT.isLeaf()) {
                int keyDistance = data.getKeyRTDistance();
                Role role = null;
                assert pos == KeyPosition.GREATER || pos == KeyPosition.LESS;
                if (pos == KeyPosition.GREATER) role = Role.RIGHT_RT;
                else role = Role.LEFT_RT;

                if (keyDistance < 0) keyDistance = coreRT.size(role) + 1;

                if (keyDistance > 1) {
                    keyDistance--;
                    targetNodeId = coreRT.get(role, keyDistance - 1);
                    System.out.println("KEY DISTANCE: " + keyDistance + " vs " +
                            role + " Size: " + coreRT.size(role) + " for RT: " +
                            coreRT);
                    assert keyDistance <= coreRT.size(role);
                    assert keyDistance >= 1;
                    assert targetNodeId != RoutingTable.DEF_VAL;
                }
                else {
                    assert keyDistance == 1;
                    if (pos == KeyPosition.GREATER) {
                        targetNodeId = coreRT.get(Role.LAST_BUCKET_NODE);
                        data.addToQueue(coreRT.get(Role.RIGHT_A_NODE));
                        data.addToQueue(coreRT.get(Role.RIGHT_RT, 0));
                        assert targetNodeId != RoutingTable.DEF_VAL;
                    }
                    else {
                        assert pos == KeyPosition.LESS;
                        if (coreRT.isEmpty(Role.LEFT_RT)) {
                            resolveSubrequest(msg);
                            return;
                        }
                        targetNodeId = coreRT.get(Role.LEFT_RT, 0);
                        assert targetNodeId != RoutingTable.DEF_VAL;
                    }
                    assert targetNodeId != RoutingTable.DEF_VAL;
                }

                msg.setDestinationId(targetNodeId);
                data.setKeyPosition(pos);
                data.setKeyRTDistance(keyDistance);
                msg.setData(data);
                send(msg);
            }
            else {
                assert coreRT.isBucketNode();
                if (pos == KeyPosition.LESS) {
                    if (coreRT.isEmpty(Role.LEFT_RT)) {
                        targetNodeId = id;
                        // msg.setDestinationId(targetNodeId);
                        resolveSubrequest(msg);
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
                        send(msg);
                    }
                    else {
                        resolveSubrequest(msg);
                        return;
                    }
                }
            }
        }
        else {
            slowLookup(msg, key, targetNodeId);
        }
    }

    void slowLookup(Message msg, long key, long targetNodeId) {

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
        send(msg);
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
        long key = data.getKey();
        this.decreasePendingQueries();
        boolean keyExists = keys.contains(key);
        if (msg.getType() == MessageT.INSERT_REQ) {
            if (!keyExists) keys.add(key);
            InsertResponse rData = new InsertResponse(key, keyExists, msg);
            if (dest == id) lookupResponse();
            else send(new Message(id, dest, rData));
        }
        else if (msg.getType() == MessageT.DELETE_REQ) {
            if (keyExists) keys.remove(key);
            DeleteResponse rData = new DeleteResponse(key, keyExists, msg);
            if (dest == id) lookupResponse();
            else send(new Message(id, dest, rData));
        }
        else {
            assert msg.getType() == MessageT.LOOKUP_REQ;
            LookupResponse rData = new LookupResponse(key, keyExists, msg);
            if (dest == id) lookupResponse();
            else send(new Message(id, dest, rData));
        }
        String printText = String
                .format("Request of type %s for key %d has reached target %d after %d hops",
                        MessageT.toString(msg.getType()), key, id,
                        msg.getHops());
        PrintMessage.print(msg, printText, indexLogFile);

    }

    void addValue(long value) {
        this.keys.add(value);
    }

    boolean directionHasChanged(long key, KeyPosition pos) {
        if (keys.isEmpty()) return false;
        long minRange = Collections.min(keys);
        long maxRange = Collections.max(keys);
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
            send(msg);
        }
        else {
            long unevenSubtree = sourceId;
            VWUpdateResponse rData = new VWUpdateResponse(unevenSubtree);
            send(new Message(id, data.getOriginalNode(), rData));
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
        if (data.getMode() == Mode.MANUAL) {
            forwardManualKeyReplacementRequest(msg, rt);
        }
        else if (data.getMode() == Mode.INORDER ||
                data.getMode() == Mode.REVERSE_INORDER) {
            forwardInorderKeyReplacementRequest(msg, rt);
        }
    }

    private void forwardInorderKeyReplacementRequest(Message msg,
            RoutingTable rt) {
        KeyReplacementRequest data = (KeyReplacementRequest) msg.getData();
        // ArrayList<Long> tempKeys;
        long destinationID = msg.getDestinationId();
        // tempKeys = new ArrayList<Long>(this.keys);
        TreeSet<Long> movingKeys;
        TreeSet<Long> stayingKeys;

        assert !data.getKeys().isEmpty();
        // assert !keys.isEmpty() || !legacyKeys.isEmpty();
        if (data.getDestinationPeer() == id) {
            // assert keys.isEmpty();
            this.keys.addAll(new TreeSet<Long>(data.getKeys()));
            this.legacyKeys.clear();
            return;
        }
        if (keys.isEmpty()) {
            assert !legacyKeys.isEmpty();
            movingKeys = new TreeSet<Long>(data.getKeys());
            stayingKeys = new TreeSet<Long>();
        }
        else if (data.getMode() == Mode.INORDER) {
            // assert this.keys.get(0) > data.getKeys().get(0);
            // TreeSet<Long> allKeys = new TreeSet<Long>(data.getKeys());
            // allKeys.addAll(this.keys);
            ArrayList<Long> allKeys = new ArrayList<Long>(data.getKeys());
            allKeys.addAll(this.keys);
            Collections.sort(allKeys);

            int movingSize = data.getKeys().size();
            int stayingSize = allKeys.size() - movingSize;
            stayingKeys = new TreeSet<Long>(allKeys.subList(0, stayingSize));
            movingKeys = new TreeSet<Long>(allKeys.subList(stayingSize,
                    stayingSize + movingSize));

            assert stayingKeys.last() <= movingKeys.first();
        }
        else {
            assert data.getMode() == Mode.REVERSE_INORDER;
            // assert this.keys.get(0) < data.getKeys().get(0);
            ArrayList<Long> allKeys = new ArrayList<Long>(data.getKeys());
            allKeys.addAll(this.keys);
            Collections.sort(allKeys);

            int movingSize = data.getKeys().size();
            int stayingSize = allKeys.size() - movingSize;
            stayingKeys = new TreeSet<Long>(allKeys.subList(movingSize,
                    movingSize + stayingSize));
            movingKeys = new TreeSet<Long>(allKeys.subList(0, movingSize));

            assert movingKeys.last() <= stayingKeys.first();
        }
        if (legacyKeys.isEmpty()) {
            assert !keys.isEmpty();
            legacyKeys.addAll(new TreeSet<Long>(this.keys));
        }
        else if (!stayingKeys.isEmpty()) legacyKeys.clear();

        assert !movingKeys.isEmpty();
        data.setKeys(movingKeys);
        this.keys = stayingKeys;

        if (data.getMode() == Mode.INORDER) {
            if (rt.isLeaf()) {
                data.reverseBucketVisits();
                if (data.visitsBuckets()) {
                    destinationID = rt.get(Role.FIRST_BUCKET_NODE);
                }
                else {
                    destinationID = rt.get(Role.RIGHT_A_NODE);
                    if (destinationID == RoutingTable.DEF_VAL) {
                        destinationID = rt.get(Role.LAST_BUCKET_NODE);
                        data.forcedInsertion(true);
                    }
                }
            }
            else if (rt.isBucketNode()) {
                if (rt.isEmpty(Role.RIGHT_RT)) {
                    if (data.insertionIsForced()) {
                        this.keys.addAll(movingKeys);
                        assert !keys.isEmpty() || !legacyKeys.isEmpty();
                        return;
                    }
                    else destinationID = rt.get(Role.REPRESENTATIVE);
                }
                else destinationID = rt.get(Role.RIGHT_RT, 0);
            }
            else {
                destinationID = rt.get(Role.RIGHT_A_NODE);
            }
        }
        else {
            assert data.getMode() == Mode.REVERSE_INORDER;
            if (rt.isLeaf()) {
                data.reverseBucketVisits();
                if (data.visitsBuckets()) {
                    destinationID = rt.get(Role.LAST_BUCKET_NODE);
                }
                else {
                    destinationID = rt.get(Role.LEFT_A_NODE);
                    if (destinationID == RoutingTable.DEF_VAL) {
                        this.keys.addAll(movingKeys);
                        assert !keys.isEmpty() || !legacyKeys.isEmpty();
                        return;
                    }
                }
            }
            else if (rt.isBucketNode()) {
                if (rt.isEmpty(Role.LEFT_RT)) destinationID = rt
                        .get(Role.REPRESENTATIVE);
                else destinationID = rt.get(Role.LEFT_RT, 0);
            }
            else {
                destinationID = rt.get(Role.LEFT_A_NODE);
            }
            // send(new Message(id, destinationID, data));
        }
        assert !keys.isEmpty() || !legacyKeys.isEmpty();
        send(new Message(id, destinationID, data));
    }

    private void forwardManualKeyReplacementRequest(Message msg, RoutingTable rt) {
        KeyReplacementRequest data = (KeyReplacementRequest) msg.getData();
        boolean newKeysAreComing = !data.getKeys().isEmpty();
        if (data.getDestinationPeer() == id) {
            assert newKeysAreComing;
            if (newKeysAreComing) {
                this.legacyKeys = new TreeSet<Long>(keys);
                this.keys = new TreeSet<Long>(data.getKeys());
            }
        }
        else if (newKeysAreComing) {
            msg.setDestinationId(data.getDestinationPeer());
            send(msg);
        }
        else { // destination != id and new keys are not coming
            if (!this.keys.isEmpty() && !this.legacyKeys.isEmpty()) {
                data.setKeys(new TreeSet<Long>(legacyKeys));
                this.legacyKeys = new TreeSet<Long>();

                msg.setData(data);
                msg.setDestinationId(data.getDestinationPeer());
                send(msg);
            }
            else if (!this.keys.isEmpty()) {
                this.legacyKeys = new TreeSet<Long>(this.keys);
                this.keys = new TreeSet<Long>();

                data.setKeys(new TreeSet<Long>(legacyKeys));
                msg.setData(data);
                msg.setDestinationId(data.getDestinationPeer());
                send(msg);
            }
        }
    }

    private boolean keyIsInRange(double key) {
        if (keys.isEmpty()) return false;
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

    void send(Message msg) {
        assert msg.getDestinationId() != this.id;
        assert msg.getDestinationId() != RoutingTable.DEF_VAL;
        net.sendMsg(msg);
    }

    public void lookupResponse() {
        decreasePendingQueries();
    }
}
