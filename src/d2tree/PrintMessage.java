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
public class PrintMessage extends MessageBody {
	private static final long serialVersionUID = -6662495188045778809L;

	private boolean down;
	private long initialNode;
	
	public PrintMessage(boolean down, long initialNode) {
		this.down = down;
		this.initialNode = initialNode;
    }

	long getInitialNode(){
		return initialNode;
	}
	public boolean goesDown(){
		return this.down;
	}
    @Override
    public int getType() {
        return D2TreeMessageT.PRINT_MSG;
    }
}
