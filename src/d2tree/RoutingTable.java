/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package d2tree;

import java.io.PrintWriter;
import java.util.Vector;

/**
 *
 * @author Pavlos Melissinos
 */
public class RoutingTable {
	public static Long DEF_VAL = -1L;
	public static enum Role {
		LEFT_CHILD,
		RIGHT_CHILD,
		PARENT,
		LEFT_NEIGHBOR,
		RIGHT_NEIGHBOR,
		LEFT_RT,
		RIGHT_RT,
		LEFT_A_NODE,
		RIGHT_A_NODE,
		BUCKET_NODE,
		REPRESENTATIVE;
	};

	//base tree
	private long leftChild;
	private long rightChild;
	private long parent;
	
	//level groups
	private Vector<Long> leftRT;
	private Vector<Long> rightRT;

	//adjacency
	private long leftAdjacentNode;
	private long rightAdjacentNode;
	
	//buckets
	private long bucketNode;
	private long representative;
    
    RoutingTable() {
    	this.leftChild = DEF_VAL;
        this.rightChild = DEF_VAL;
        this.parent = DEF_VAL;
        
        this.leftRT = new Vector<Long>();
        this.rightRT = new Vector<Long>();
        
        this.leftAdjacentNode = DEF_VAL;
        this.rightAdjacentNode = DEF_VAL;
        
        this.bucketNode = DEF_VAL;
        this.representative = DEF_VAL;
    }
    void set(Role role, int index, long value){
    	if (value == DEF_VAL) return;
    	switch (role){
		case LEFT_RT:
			while (index >= leftRT.size()) leftRT.add(DEF_VAL);
    		if (leftRT.get(index) == DEF_VAL)
				leftRT.set(index, value);
			break;
		case RIGHT_RT:
			while (index >= rightRT.size()) rightRT.add(DEF_VAL);
    		if (rightRT.get(index) == DEF_VAL)
				rightRT.set(index, value);
			break;
		default:
			set(role, value);
    	}
    }
    void set(Role role, long value){
    	if (value == DEF_VAL) return;
    	switch (role){
    	case BUCKET_NODE: 
			this.bucketNode = value; break;
		case REPRESENTATIVE:
	        this.representative = value; break;
		case LEFT_A_NODE:
	        this.leftAdjacentNode = value; break;
		case RIGHT_A_NODE:
	        this.rightAdjacentNode = value; break;
		case LEFT_CHILD:
	        this.leftChild = value; break;
		case RIGHT_CHILD:
	        this.rightChild = value; break;
		case PARENT:
	        this.parent = value; break;
		case LEFT_NEIGHBOR:
			if (!leftRT.isEmpty())
				leftRT.set(0, value);
			else
				leftRT.add(value);
			break;
		case RIGHT_NEIGHBOR:
			if (!rightRT.isEmpty())
				rightRT.set(0, value);
			else
				rightRT.add(value);
			break;
		default:
			break;
    	}
    }

    void setLeftRT(Vector<Long> values) {
    	this.leftRT = values;
    }
    
    void setRightRT(Vector<Long> values) {
    	this.rightRT = values;
    }

    long get(Role role){
    	long value = 0;
    	switch (role){
    	case BUCKET_NODE: 
			value = this.bucketNode; break;
		case REPRESENTATIVE:
			value = this.representative; break;
		case LEFT_A_NODE:
			value = this.leftAdjacentNode; break;
		case RIGHT_A_NODE:
			value = this.rightAdjacentNode; break;
		case LEFT_CHILD:
			value = this.leftChild; break;
		case RIGHT_CHILD:
			value = this.rightChild; break;
		case PARENT:
			value = this.parent; break;
		case LEFT_NEIGHBOR:
			value = leftRT.firstElement(); break;
		case RIGHT_NEIGHBOR:
			value = rightRT.firstElement(); break;
		default:
			try {
				throw new Exception();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} break;
    	}
		return value;
    }
    
    Vector<Long> getLeftRT() {
        return this.leftRT;
    }
    
    Vector<Long> getRightRT() {
        return this.rightRT;
    }
    
    int size() {
    	int size = 7;
    	size += leftRT.size();
    	size += rightRT.size();
        return size;
    }
    long getHeight(){
    	return Math.max(leftRT.size(), rightRT.size()) + 1;
    }
    void print(PrintWriter out){
//		out.println("P = " + getParent() +
//				 ", LC = " + getLeftChild() +
//				 ", RC = " + getRightChild() +
//				 ", LA = " + getLeftAdjacentNode() +
//				 ", RA = " + getRightAdjacentNode() +
//				", LRT = " + getLeftRT() +
//				", RRT = " + getRightRT() +
//				 ", BN = " + getBucketNode() +
//				 ", RN = " + getRepresentative());
        out.format( "P = %3d, LC = %3d, RC = %3d, LA = %3d, RA = %3d, BN = %3d, RN = %3d, LRT = [",
        		get(Role.PARENT), get(Role.LEFT_CHILD), get(Role.RIGHT_CHILD), get(Role.LEFT_A_NODE), get(Role.RIGHT_A_NODE),
        		get(Role.BUCKET_NODE), get(Role.REPRESENTATIVE));
        for (Long node : getLeftRT()){
        	out.format("%3d ", node);
        }
        //if (getLeftRT().isEmpty())  out.print("    ");
        out.print("], RRT = [");
        for (Long node : getRightRT()){
        	out.format("%3d ", node);
        }
        //if (getRightRT().isEmpty()) out.print("    ");
        out.println("]");
        
		//out.println(", LRT = " + getLeftRT() + ", RRT = " + getRightRT());
    }
}
