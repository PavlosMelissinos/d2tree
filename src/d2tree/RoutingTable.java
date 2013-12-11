/*
 * To change this template, choose Tools | Templates and open the template in
 * the editor.
 */

package d2tree;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;

import p2p.simulator.message.Message;

/**
 * 
 * @author Pavlos Melissinos
 */
public class RoutingTable {
    public static Long DEF_VAL = -1L;

    public static enum Role {
        // base tree
        LEFT_CHILD,
        RIGHT_CHILD,
        PARENT,
        // level groups
        LEFT_RT,
        RIGHT_RT,
        // adjacency
        LEFT_A_NODE,
        RIGHT_A_NODE,
        // buckets
        FIRST_BUCKET_NODE,
        LAST_BUCKET_NODE,
        REPRESENTATIVE;

        static Role mirrorRole(Role role) {
            Role reverseRole = null;
            if (role == FIRST_BUCKET_NODE || role == LAST_BUCKET_NODE) reverseRole = REPRESENTATIVE;
            else if (role == PARENT) reverseRole = LEFT_CHILD;
            else if (role == LEFT_CHILD || role == RIGHT_CHILD) reverseRole = PARENT;
            else if (role == LEFT_A_NODE) reverseRole = RIGHT_A_NODE;
            else if (role == RIGHT_A_NODE) reverseRole = LEFT_A_NODE;
            else if (role == REPRESENTATIVE) reverseRole = FIRST_BUCKET_NODE;
            else if (role == LEFT_RT) reverseRole = RIGHT_RT;
            else if (role == RIGHT_RT) reverseRole = LEFT_RT;
            return reverseRole;
        }

        static Role mirrorRole2(Role role) {
            Role reverseRole = null;
            if (role == PARENT) reverseRole = RIGHT_CHILD;
            else if (role == REPRESENTATIVE) reverseRole = LAST_BUCKET_NODE;
            else reverseRole = Role.mirrorRole(role);
            return reverseRole;
        }

    };

    private ArrayList<Long>                          leftRT;
    private ArrayList<Long>                          rightRT;
    private HashMap<Role, Long>                      visiblePeers;
    private long                                     id;
    public static HashMap<Long, HashMap<Role, Long>> discrepancies;

    RoutingTable(long id) {
        this.leftRT = new ArrayList<Long>();
        this.rightRT = new ArrayList<Long>();
        this.visiblePeers = new HashMap<Role, Long>();
        this.id = id;
        if (discrepancies == null)
            discrepancies = new HashMap<Long, HashMap<Role, Long>>();
        discrepancies.put(id, new HashMap<Role, Long>());
    }

    void set(Role role, int index, long value) {
        long oldValue = this.get(role, index);
        if (value == DEF_VAL) {
            new IllegalArgumentException().printStackTrace();
            unset(role, index, get(role, index));
            return;
        }
        switch (role) {
        case LEFT_RT:
            while (index >= leftRT.size())
                leftRT.add(DEF_VAL);
            if (leftRT.get(index) == DEF_VAL) leftRT.set(index, value);
            break;
        case RIGHT_RT:
            while (index >= rightRT.size())
                rightRT.add(DEF_VAL);
            if (rightRT.get(index) == DEF_VAL) rightRT.set(index, value);
            break;
        default:
            set(role, value);
            return;
        }
        updateInconsistencies(role, index, oldValue);
        String printText = id + "." + role + "=" + value + "(replaced " +
                oldValue + ")";
        PrintMessage data = new PrintMessage(D2TreeMessageT.CONNECT_MSG, id);
        PrintMessage.print(new Message(id, id, data), printText,
                PrintMessage.logDir + "conn-disconn.txt");
        if (value == id) throw new IllegalArgumentException();
    }

    void set(Role role, long value) throws IllegalArgumentException {
        assert role != Role.LEFT_RT && role != Role.RIGHT_RT;
        long oldValue = this.get(role);
        if (value == DEF_VAL) {
            throw new IllegalArgumentException();
        }
        else this.visiblePeers.put(role, value);
        updateInconsistencies(role, 0, oldValue);
        String printText = id + "." + role + "=" + value + "(replaced " +
                oldValue + ")";
        PrintMessage data = new PrintMessage(D2TreeMessageT.CONNECT_MSG, id);
        PrintMessage.print(new Message(id, id, data), printText,
                PrintMessage.logDir + "conn-disconn.txt");
        if (value == id) throw new IllegalArgumentException();
    }

    void unset(Role role, int index, long oldValue) {
        switch (role) {
        case LEFT_RT:
            if (leftRT.size() > index && leftRT.get(index) == oldValue) {
                leftRT.remove(index);
                updateInconsistencies(role, index, oldValue);
            }
            break;
        case RIGHT_RT:
            if (rightRT.size() > index && rightRT.get(index) == oldValue) {
                rightRT.remove(index);
                updateInconsistencies(role, index, oldValue);
            }
            break;
        default:
            unset(role, oldValue);
            return;
        }
        String printText = id + "." + role + "=" + DEF_VAL + "(replaced " +
                oldValue + ")";
        PrintMessage data = new PrintMessage(D2TreeMessageT.CONNECT_MSG, id);
        PrintMessage.print(new Message(id, id, data), printText,
                PrintMessage.logDir + "conn-disconn.txt");
    }

    void unset(Role role, long oldValue) {
        assert role != Role.LEFT_RT && role != Role.RIGHT_RT;
        if (this.visiblePeers.containsKey(role) &&
                this.visiblePeers.get(role) == oldValue) {
            this.visiblePeers.remove(role);
            updateInconsistencies(role, 0, oldValue);
        }
        String printText = id + "." + role + "=" + DEF_VAL + "(replaced " +
                oldValue + ")";
        PrintMessage data = new PrintMessage(D2TreeMessageT.CONNECT_MSG, id);
        PrintMessage.print(new Message(id, id, data), printText,
                PrintMessage.logDir + "conn-disconn.txt");
    }

    void unset(Role role) {
        assert role == Role.LEFT_RT || role == Role.RIGHT_RT;
        ArrayList<Long> oldRT = null;
        if (role == Role.LEFT_RT) {
            oldRT = leftRT;
            leftRT.clear();
        }
        else if (role == Role.RIGHT_RT) {
            oldRT = rightRT;
            rightRT.clear();
        }
        for (int i = 0; i < oldRT.size(); i++) {
            long peer = oldRT.get(i);
            updateInconsistencies(role, i, peer);
        }
        String printText = id + "." + role + "=" + new ArrayList<Long>() +
                "(replaced " + oldRT + ")";
        PrintMessage data = new PrintMessage(D2TreeMessageT.CONNECT_MSG, id);
        PrintMessage.print(new Message(id, id, data), printText,
                PrintMessage.logDir + "conn-disconn.txt");
    }

    // void setRT(Role role, ArrayList<Long> values){
    // if (role == Role.LEFT_RT){
    // this.leftRT = values;
    // }
    // else if (role == Role.RIGHT_RT){
    // this.rightRT = values;
    // }
    // else throw new IllegalArgumentException();
    // }
    // void setLeftRT(Vector<Long> values) {
    // this.leftRT = values;
    // }
    //
    // void setRightRT(Vector<Long> values) {
    // this.rightRT = values;
    // }
    long get(Role role, int index) {
        long value = DEF_VAL;
        if (role == Role.LEFT_RT) {
            if (index < this.leftRT.size()) value = this.leftRT.get(index);
        }
        else if (role == Role.RIGHT_RT) {
            if (index < this.rightRT.size()) value = this.rightRT.get(index);
        }
        else value = get(role);
        return value;
    }

    long get(Role role) {
        assert role != Role.LEFT_RT && role != Role.RIGHT_RT;
        return visiblePeers.containsKey(role) ? this.visiblePeers.get(role)
                : DEF_VAL;
    }

    boolean contains(Role role, int index) {
        boolean exists = false;
        if (role == Role.LEFT_RT) exists = leftRT.size() > index &&
                get(role, index) != DEF_VAL;
        else if (role == Role.RIGHT_RT) exists = rightRT.size() > index &&
                get(role, index) != DEF_VAL;
        else exists = contains(role);
        return exists;
    }

    boolean contains(Role role) {
        assert role != Role.LEFT_RT && role != Role.RIGHT_RT;
        return visiblePeers.containsKey(role) &&
                visiblePeers.get(role) != DEF_VAL;
    }

    boolean isEmpty(Role role) {
        if (role == Role.LEFT_RT) return this.leftRT.isEmpty();
        else if (role == Role.RIGHT_RT) return this.rightRT.isEmpty();
        else throw new IllegalArgumentException();
    }

    int size() {
        int size = visiblePeers.size();
        size += leftRT.size();
        size += rightRT.size();
        return size;
    }

    int size(Role role) {
        if (role == Role.LEFT_RT) return this.leftRT.size();
        else if (role == Role.RIGHT_RT) return this.rightRT.size();
        else throw new IllegalArgumentException();
    }

    long getHeight() {
        return Math.max(leftRT.size(), rightRT.size()) + 1;
    }

    long getRandomRTNode() {
        int max = size(Role.LEFT_RT) + size(Role.RIGHT_RT);
        double rand = Math.floor(Math.random() * max);
        int index = Math.round((float) rand) - size(Role.LEFT_RT);
        if (index < 0) return get(Role.LEFT_RT, -index - 1);
        else return get(Role.RIGHT_RT, index);
    }

    void print(PrintWriter out) {
        out.print(this);
    }

    private boolean isConsistent(Role role, int index) {
        if (!contains(role, index)) return true;
        long peer = get(role, index);
        RoutingTable peerRT = D2TreeCore.routingTables.get(peer);
        Role mirrorRole = Role.mirrorRole(role);
        Role mirrorRole2 = Role.mirrorRole2(role);
        Long mirrorPeer = peerRT.get(mirrorRole, index);
        Long mirrorPeer2 = peerRT.get(mirrorRole2, index);
        if (role == Role.REPRESENTATIVE)
            return getBucketNodes(peer).contains(id) || mirrorPeer == id ||
                    mirrorPeer2 == id;
        return mirrorPeer == id || mirrorPeer2 == id;
    }

    private boolean wasConsistent(Role role, int index, long oldPeer) {
        HashMap<Role, Long> disc = discrepancies.get(id);
        // HashMap<Role, Long> oldPeerDiscrepancies =
        // discrepancies.get(oldPeer);
        if (oldPeer == DEF_VAL) return !disc.containsKey(role);
        RoutingTable oldPeerRT = D2TreeCore.routingTables.get(oldPeer);
        Role mirrorRole = Role.mirrorRole(role);
        Long mirrorPeer = oldPeerRT.get(mirrorRole, index);
        Role mirrorRole2 = Role.mirrorRole2(role);
        Long mirrorPeer2 = oldPeerRT.get(mirrorRole2, index);
        Long inconsistentPeer = disc.get(role);
        // assert inconsistentPeer != null;
        assert mirrorPeer != null;
        assert mirrorPeer2 != null;
        if (inconsistentPeer != Long.valueOf(oldPeer) &&
                mirrorPeer != Long.valueOf(id)) return true;
        if (inconsistentPeer == Long.valueOf(oldPeer) &&
                mirrorPeer == Long.valueOf(id)) return true;
        if (inconsistentPeer != Long.valueOf(oldPeer) &&
                mirrorPeer2 != Long.valueOf(id)) return true;
        if (inconsistentPeer == Long.valueOf(oldPeer) &&
                mirrorPeer2 == Long.valueOf(id)) return true;
        return false;

    }

    private void updateInconsistencies(Role role, int index, long oldPeer) {
        long peer = get(role, index);
        // RoutingTable peerRT = D2TreeCore.routingTables.get(peer);
        Role mirrorRole = Role.mirrorRole(role);
        Role mirrorRole2 = Role.mirrorRole2(role);

        if (peer == oldPeer) return;
        updateInconsistencies(id, role, index, oldPeer);
        updateInconsistencies(peer, mirrorRole, index, DEF_VAL);
        updateInconsistencies(peer, mirrorRole2, index, DEF_VAL);
        updateInconsistencies(oldPeer, mirrorRole, index, DEF_VAL);
        updateInconsistencies(oldPeer, mirrorRole2, index, DEF_VAL);
        printDiscrepancies();
    }

    private void updateInconsistencies(long myPeer, Role role, int index,
            long oldPeer) {
        if (myPeer == DEF_VAL) return;
        RoutingTable myPeerRT = D2TreeCore.routingTables.get(myPeer);
        long otherPeer = myPeerRT.get(role, index);
        HashMap<Role, Long> disc = discrepancies.get(myPeer);
        String printText = String.format("%d.%s = %d (replaced %d)", myPeer,
                role, otherPeer, oldPeer);
        if (disc == null) disc = new HashMap<Role, Long>();
        if (isConsistent(role, index)) {
            printText = "DISCREPANCY_FIXE: " + printText;
            disc.remove(role);
        }
        else {
            RoutingTable otherPeerRT = D2TreeCore.routingTables.get(otherPeer);
            Role mirrorRole = Role.mirrorRole(role);
            Role mirrorRole2 = Role.mirrorRole2(role);
            long mirrorPeer = otherPeerRT.get(mirrorRole, index);
            long mirrorPeer2 = otherPeerRT.get(mirrorRole2, index);
            // printText = "DISCREPANCY_ADDE: " + printText + " whereas " +
            // oldPeer + "." + mirrorRole + " = " + mirrorPeer + " and " +
            // oldPeer + "." + mirrorRole2 + " = " + mirrorPeer2;
            printText = String
                    .format("DISCREPANCY_ADDE: %s whereas %d.%s = %d / %d.%s = %d (new peer) and %d.%s = %d / %d.%s = %d (old peer)",
                            printText, otherPeer, mirrorRole, mirrorPeer,
                            otherPeer, mirrorRole2, mirrorPeer2, oldPeer,
                            mirrorRole, mirrorPeer, oldPeer, mirrorRole2,
                            mirrorPeer2);
            disc.put(role, otherPeer);
        }

        if (disc.isEmpty()) discrepancies.remove(myPeer);
        else discrepancies.put(myPeer, disc);

        String logFile = PrintMessage.logDir + "errors.txt";
        PrintMessage data = new PrintMessage(D2TreeMessageT.PRINT_ERR_MSG, id);
        Message msg = new Message(id, id, data);
        PrintMessage.print(msg, printText, logFile, data.getInitialNode());
    }

    static ArrayList<Long> getBucketNodes(long bucket) {
        ArrayList<Long> bucketNodes = new ArrayList<Long>();
        RoutingTable rt = D2TreeCore.routingTables.get(bucket);
        long bucketNode = rt.get(Role.FIRST_BUCKET_NODE);
        while (bucketNode != DEF_VAL && !bucketNodes.contains(bucketNode)) {
            bucketNodes.add(bucketNode);
            rt = D2TreeCore.routingTables.get(bucketNode);
            bucketNode = rt.get(Role.RIGHT_RT, 0);
        }
        return bucketNodes;
    }

    private synchronized void printDiscrepancies() {
        String logFile = PrintMessage.logDir + "errors.txt";
        PrintMessage data = new PrintMessage(D2TreeMessageT.PRINT_ERR_MSG, id);
        Message msg = new Message(id, id, data);
        PrintMessage.print(msg, discrepancies.toString(), logFile);
    }

    public String toString() {
        String lRT = "";
        for (int index = leftRT.size() - 1; index >= 0; index--) {
            long node = leftRT.get(index);
            lRT += node + ", ";
        }
        String rRT = "";
        for (int index = 0; index < rightRT.size(); index++) {
            long node = rightRT.get(index);
            rRT += ", " + node;
        }
        String rt = String
                .format("P = %3d, LC = %3d, RC = %3d, LA = %3d, RA = %3d, FBN = %3d, LBN = %3d, RN = %3d, RT = [%s-%s]",
                        get(Role.PARENT), get(Role.LEFT_CHILD),
                        get(Role.RIGHT_CHILD), get(Role.LEFT_A_NODE),
                        get(Role.RIGHT_A_NODE), get(Role.FIRST_BUCKET_NODE),
                        get(Role.LAST_BUCKET_NODE), get(Role.REPRESENTATIVE),
                        lRT, rRT);
        return rt;
    }
}
