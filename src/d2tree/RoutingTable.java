/*
 * To change this template, choose Tools | Templates and open the template in
 * the editor.
 */

package d2tree;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;

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
            switch (role) {
            case FIRST_BUCKET_NODE:
                reverseRole = REPRESENTATIVE;
                break;
            case LAST_BUCKET_NODE:
                reverseRole = REPRESENTATIVE;
                break;
            case PARENT:
                reverseRole = LEFT_CHILD;
                break;
            case LEFT_CHILD:
                reverseRole = PARENT;
                break;
            case RIGHT_CHILD:
                reverseRole = PARENT;
                break;
            case LEFT_A_NODE:
                reverseRole = RIGHT_A_NODE;
                break;
            case RIGHT_A_NODE:
                reverseRole = LEFT_A_NODE;
                break;
            case REPRESENTATIVE:
                reverseRole = FIRST_BUCKET_NODE;
                break;
            case LEFT_RT:
                reverseRole = RIGHT_RT;
                break;
            case RIGHT_RT:
                reverseRole = LEFT_RT;
                break;
            default:
                reverseRole = null;
                break;
            }
            return reverseRole;
        }

        static Role mirrorRole2(Role role) {
            Role reverseRole = null;
            switch (role) {
            case PARENT:
                reverseRole = RIGHT_CHILD;
                break;
            case REPRESENTATIVE:
                reverseRole = LAST_BUCKET_NODE;
                break;
            default:
                reverseRole = null;
                break;
            }
            return reverseRole;
        }

    };

    private ArrayList<Long>     leftRT;
    private ArrayList<Long>     rightRT;
    private HashMap<Role, Long> visiblePeers;

    RoutingTable() {
        this.leftRT = new ArrayList<Long>();
        this.rightRT = new ArrayList<Long>();
        this.visiblePeers = new HashMap<Role, Long>();
    }

    void set(Role role, int index, long value) {
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
        }
    }

    void set(Role role, long value) throws IllegalArgumentException {
        assert role != Role.LEFT_RT && role != Role.RIGHT_RT;
        if (value == DEF_VAL) {
            throw new IllegalArgumentException();
        }
        else this.visiblePeers.put(role, value);
    }

    void unset(Role role, int index, long oldValue) {
        switch (role) {
        case LEFT_RT:
            if (leftRT.size() > index && leftRT.get(index) == oldValue)
                leftRT.remove(index);
            break;
        case RIGHT_RT:
            if (rightRT.size() > index && rightRT.get(index) == oldValue)
                rightRT.remove(index);
            break;
        default:
            unset(role, oldValue);
        }
    }

    void unset(Role role, long oldValue) {
        assert role != Role.LEFT_RT && role != Role.RIGHT_RT;
        if (this.visiblePeers.containsKey(role) &&
                this.visiblePeers.get(role) == oldValue)
            this.visiblePeers.remove(role);
    }

    void unset(Role role) {
        assert role == Role.LEFT_RT || role == Role.RIGHT_RT;
        if (role == Role.LEFT_RT) leftRT.clear();
        else if (role == Role.RIGHT_RT) rightRT.clear();
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
        if (role == Role.LEFT_RT) return this.leftRT.contains(index);
        else if (role == Role.RIGHT_RT) return this.rightRT.contains(index);
        else return contains(role);
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
        int size = 7;
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

    void print(PrintWriter out) {
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
        out.format(
                "P = %3d, LC = %3d, RC = %3d, LA = %3d, RA = %3d, FBN = %3d, LBN = %3d, RN = %3d, RT = [%s-%s]",
                // "P = %3d, LC = %3d, RC = %3d, LA = %3d, RA = %3d, FBN = %3d, LBN = %3d, RN = %3d, LRT = [",
                get(Role.PARENT), get(Role.LEFT_CHILD), get(Role.RIGHT_CHILD),
                get(Role.LEFT_A_NODE), get(Role.RIGHT_A_NODE),
                get(Role.FIRST_BUCKET_NODE), get(Role.LAST_BUCKET_NODE),
                get(Role.REPRESENTATIVE), lRT, rRT);
        // out.print("], RRT = [");
        // TODO changes in routing tables
    }
}
