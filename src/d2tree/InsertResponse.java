/*
 * To change this template, choose Tools | Templates and open the template in
 * the editor.
 */

package d2tree;

import p2p.simulator.message.Message;
import p2p.simulator.message.MessageT;

/**
 * This class creates a special lookup response message body. This special
 * message is used to extract statistics regarding the protocol's lookup
 * algorithm.
 * 
 */
public class InsertResponse extends LookupResponse {
    private static final long serialVersionUID = 1210386207088539544L;

    /**
     * Creates the body message of an insert response message.
     * 
     * @param key
     *            The key of the lookup request.
     * @param exist
     *            If the key is found is true, otherwise false.
     * @param msg
     *            The initial lookup request message.
     */
    public InsertResponse(double key, boolean exist, Message msg) {
        super(key, exist, msg);
    }

    /**
     * Returns the message's type.
     * 
     * @return The type of the message
     */
    @Override
    public int getType() {
        return MessageT.INSERT_RES;
    }

    public String toString() {
        String str;

        str = "Key: " + key + " inserted: " + keyExist + " hops: " + hops;

        return str;

    }
}
