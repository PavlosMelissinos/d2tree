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
public class PrintMessage extends MessageBody {
    private static final long serialVersionUID = -6662495188045778809L;

    private boolean           down;
    private long              initialNode;
    private int               msgType;

    // public PrintMessage(boolean down, int msgType, long initialNode) {
    // this.down = down;
    public PrintMessage(int msgType, long initialNode) {
        this.initialNode = initialNode;
        this.msgType = msgType;
    }

    long getInitialNode() {
        return initialNode;
    }

    // public boolean goesDown(){
    // return this.down;
    // }
    public int getSourceType() {
        return this.msgType;
    }

    @Override
    public int getType() {
        return D2TreeMessageT.PRINT_MSG;
    }
}
