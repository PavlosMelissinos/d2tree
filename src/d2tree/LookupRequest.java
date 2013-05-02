package d2tree;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

import p2p.simulator.message.MessageBody;

/**
 *
 * @author Pavlos Melissinos
 */
public class LookupRequest extends MessageBody {
	private static final long serialVersionUID = 6809029793982822930L;
	private long key;

    public LookupRequest() {

    }
    
    public LookupRequest(long key) {
        this.key = key;
    }

    public long getKey() {
        return this.key;
    }
    
    @Override
    public int getType() {
        return D2TreeMessageT.LOOKUP_REQ;
    }


}
