/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package d2tree;

import p2p.simulator.message.MessageBody;

/**
 *
 * @author Pavlos Melissinos
 */
public class JoinResponse extends MessageBody {
	private static final long serialVersionUID = 2277970268976064781L;

	JoinResponse() {
        
    }

    @Override
    public int getType() {
        return D2TreeMessageT.JOIN_RES;
    }
    
}
