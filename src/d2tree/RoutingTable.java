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
	public static int DEF_VAL = -1;

	//base tree
	private long leftChild;
	private long rightChild;
	private long parent;
	
	//level groups
	private Vector<Long> leftRoutingTable;
	private Vector<Long> rightRoutingTable;

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
        
        this.leftRoutingTable = new Vector<Long>();
        this.rightRoutingTable = new Vector<Long>();
        
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
    
    void setLeftRoutingTable(Vector<Long> values) {
    	this.leftRoutingTable = values;
    }
    Vector<Long> getLeftRoutingTable() {
    	return this.leftRoutingTable;
    }
    void setRightRoutingTable(Vector<Long> values) {
    	this.rightRoutingTable = values;
    }
    Vector<Long> getRightRoutingTable() {
    	return this.rightRoutingTable;
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
    	size += leftRoutingTable.size();
    	size += rightRoutingTable.size();
        return size;
    }
}
