/*
 * To change this template, choose Tools | Templates and open the template in
 * the editor.
 */

package d2tree;

import p2p.simulator.message.MessageBody;

/**
 * 
 * @author Pavlos Melissinos
 */
public class VWUpdateResponse extends MessageBody {

    private final long unevenSubtree;

    public VWUpdateResponse(long unevenSubtree) {
        this.unevenSubtree = unevenSubtree;
    }

    long getUnevenSubtree() {
        return this.unevenSubtree;
    }

    @Override
    public int getType() {
        return D2TreeMessageT.VWUPDATE_RES;
    }

    private static final long serialVersionUID = 8046478217642736509L;
}
