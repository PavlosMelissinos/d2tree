/*
 * To change this template, choose Tools | Templates and open the template in
 * the editor.
 */

package d2tree;

import p2p.simulator.message.MessageBody;
import d2tree.RoutingTable.Role;

/**
 * 
 * @author Pavlos Melissinos
 */
public class LeaveRequest extends MessageBody {
    private static final long serialVersionUID = 8246114035614337373L;
    private long              leaveNodeID;
    private D2TreeCore        core;
    private D2TreeIndexCore   indexCore;
    private Role              oldRole;
    private Role              newRole;

    public LeaveRequest(long leaveNodeID) {
        this.leaveNodeID = leaveNodeID;
        oldRole = null;
        newRole = null;
    }

    public LeaveRequest(long leaveNodeID, D2TreeCore core,
            D2TreeIndexCore indexCore) {
        this(leaveNodeID);
        this.core = core;
        this.indexCore = indexCore;
    }

    void setOldRole(Role oldRole) {
        this.oldRole = oldRole;
    }

    void setNewRole(Role newRole) {
        this.newRole = newRole;
    }

    Role getOldRole() {
        return this.oldRole;
    }

    Role getNewRole() {
        return this.newRole;
    }

    public D2TreeCore getCore() {
        return core;
    }

    public D2TreeIndexCore getIndexCore() {
        return indexCore;
    }

    public long getLeaveNodeID() {
        return this.leaveNodeID;
    }

    // public static D2TreeCore buildStaticMigrateData(D2TreeCore core,
    // long newID, Role roleToReplace) {
    // core.buildMigrateData(newID, roleToReplace);
    //
    // }
    //
    // public static D2TreeIndexCore buildDynamicMigrateData(D2TreeIndexCore
    // core,
    // long newID) {
    //
    // }

    // public static long getNextNode(D2TreeCore core) {
    // long nextNode = RoutingTable.DEF_VAL;
    // RoutingTable rt = core.getRT();
    // if (!core.isLeaf() && !core.isBucketNode()) {
    // assert rt.get(Role.RIGHT_A_NODE) != RoutingTable.DEF_VAL;
    // nextNode = rt.get(Role.RIGHT_A_NODE);
    // }
    // else if (core.isLeaf()) {
    // assert rt.get(Role.FIRST_BUCKET_NODE) != RoutingTable.DEF_VAL;
    // nextNode = rt.get(Role.FIRST_BUCKET_NODE);
    // }
    // return nextNode;
    // }
    public static Role getNextRole(D2TreeCore core) {
        Role nextRole = null;
        RoutingTable rt = core.getRT();
        if (!core.isLeaf() && !core.isBucketNode()) {
            assert rt.contains(Role.RIGHT_A_NODE);
            nextRole = Role.RIGHT_A_NODE;
        }
        else if (core.isLeaf()) {
            assert rt.contains(Role.FIRST_BUCKET_NODE);
            nextRole = Role.FIRST_BUCKET_NODE;
        }
        return nextRole;
    }

    @Override
    public int getType() {
        return D2TreeMessageT.LEAVE_REQ;
    }

}
