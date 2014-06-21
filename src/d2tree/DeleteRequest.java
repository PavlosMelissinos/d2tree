/*
 * To change this template, choose Tools | Templates and open the template in
 * the editor.
 */

package d2tree;

import p2p.simulator.message.MessageT;

/**
 * 
 * @author Pavlos Melissinos
 */
public class DeleteRequest extends LookupRequest {
    private static final long serialVersionUID = -6662495188045778809L;

    // public DeleteRequest() {
    //
    // }

    public DeleteRequest(long key) {
        super(key);
    }

    @Override
    public int getType() {
        return MessageT.DELETE_REQ;
    }
}
