/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package d2tree;

import p2p.simulator.message.Message;
import p2p.simulator.message.MessageT;

/**
 * This class creates a special lookup response message body. This special message
 * is used to extract statistics regarding the protocol's lookup algorithm. 
 * 
 */
public class DeleteResponse extends LookupResponse {
	private static final long serialVersionUID = -6224280450072668801L;

	/**
     * Creates the body message of a lookup response message.
     *  
     * @param key The key of the lookup request.
     * @param exist If the key is found is true, otherwise false.
     * @param msg The initial lookup request message.
     */
    public DeleteResponse(long key, boolean exist, Message msg) {
        super(key, exist, msg);        
    }

    /**
     * Returns the message's type.
     * 
     * @return The type of the message
     */
    @Override
    public int getType() {
        return MessageT.DELETE_RES;
    }
    
    @Override
    public String toString() {
        String str;
        
        str = "Key: "+key+" deleted: "+keyExist+" hops: "+hops;
        
        return str;
            
    }
}
