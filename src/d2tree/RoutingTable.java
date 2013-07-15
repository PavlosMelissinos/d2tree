/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package d2tree;

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
    
    void setLeftChild(long lc) {
        this.leftChild = lc;
    }
    
    long getLeftChild() {
        return this.leftChild;
    }

    void setRightChild(long rc) {
        this.rightChild = rc;
    }
    
    long getRightChild() {
        return this.rightChild;
    }

    void setParent(long p) {
        this.parent = p;
    }
    
    long getParent() {
        return this.parent;
    }

    void setLeftAdjacentNode(long ln) {
        this.leftAdjacentNode = ln;
    }
    
    long getLeftAdjacentNode() {
        return this.leftAdjacentNode;
    }

    void setRightAdjacentNode(long ln) {
        this.rightAdjacentNode = ln;
    }
    
    long getRightAdjacentNode() {
        return this.rightAdjacentNode;
    }
    
    void setLeftRT(Vector<Long> values) {
    	this.leftRT = values;
    }
    Vector<Long> getLeftRT() {
    	return this.leftRT;
    }
    void setRightRT(Vector<Long> values) {
    	this.rightRT = values;
    }
    Vector<Long> getRightRT() {
    	return this.rightRT;
    }
    
    void setBucketNode(Long node){
    	this.bucketNode = node;
    }
    long getBucketNode(){
    	return this.bucketNode;
    }
    
    void setRepresentative(long node){
    	this.representative = node;
    }
    long getRepresentative(){
    	return this.representative;
    }
    
    int size() {
    	int size = 7;
    	size += leftRT.size();
    	size += rightRT.size();
        return size;
    }
    long getHeight(){ 
    	return Math.max(leftRT.size(), rightRT.size());
    }
}
