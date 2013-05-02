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
public class JoinMessage extends MessageBody {
	private static final long serialVersionUID = 3310952612779066858L;

	@Override
    public int getType() {
        return D2TreeMessageT.JOIN_REQ;
    }

}
