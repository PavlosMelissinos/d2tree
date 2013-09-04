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
public class PrintRTMessage extends MessageBody {
	private static final long serialVersionUID = -6662495188045778809L;

	private long initialNode;
	
	public PrintRTMessage(long initialNode) {
		this.initialNode = initialNode;
    }
	long getInitialNode(){
		return initialNode;
	}
    @Override
    public int getType() {
        return D2TreeMessageT.PRINT_RT_MSG;
    }
}
