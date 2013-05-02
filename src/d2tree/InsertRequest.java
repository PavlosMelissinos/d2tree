/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package d2tree;

/**
 *
 * @author Pavlos Melissinos
 */
public class InsertRequest extends LookupRequest {
	private static final long serialVersionUID = -3691206251067479109L;

	public InsertRequest() {

    }

    public InsertRequest(long key) {
        super(key);
    }
    
    @Override
    public int getType() {
        return D2TreeMessageT.INSERT_REQ;
    }
}
