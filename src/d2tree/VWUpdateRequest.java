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
public class VWUpdateRequest extends MessageBody {

    private double     vW;
    private final long height;
    private final long originalNode;

    public VWUpdateRequest(double vW, long height, long originalNode) {
        this.vW = vW;
        this.height = height;
        this.originalNode = originalNode;
    }

    double getVirtualWeight() {
        return this.vW;
    }

    void setVirtualWeight(double vW) {
        this.vW = vW;
    }

    long getHeight() {
        return this.height;
    }

    long getOriginalNode() {
        return this.originalNode;
    }

    @Override
    public int getType() {
        return D2TreeMessageT.VWUPDATE_REQ;
    }

    private static final long serialVersionUID = 8046478217642736509L;
}
