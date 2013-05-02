/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package d2tree;

import java.util.Vector;

//import D2Tree.ConnectMessage;

import p2p.simulator.message.LookupResponse;
import p2p.simulator.message.Message;
import p2p.simulator.network.Network;

/**
 *
 * @author Pavlos Melissinos
 */
public class D2TreeCore {

    private RoutingTable rt;
    private Network net;
    private long id;
    private long n;
    private long k;
    private float nc; //node criticality

    //TODO keep as singleton for simplicity, convert to Vector of keys later
    private long key;
    
    D2TreeCore(long id, long n, long k, Network net) {
    
        this.k          = k;
        this.n          = n;
        this.rt         = new RoutingTable();
        this.net        = net;
        this.id         = id;
        this.nc			= 0.5F;
    }

    /**
     * if core is leaf, then forward to first bucket node if exists, otherwise connect node
     * else if core is an inner node, then forward to nearest leaf
     * else if core is a bucket node, then forward to next bucket node until it's the last bucket node of the bucket
     * else if core is the last bucket node, then connect node
     * **/
    void forwardJoinRequest(Message msg) {

        Message m;

        long newNodeId = msg.getSourceId();
        
        //DEBUG
        System.out.println("Adding node " + newNodeId);
        System.out.println("Forwarded to node " + id);
        
        if (isBucketNode()){ //forward to next bucket node
        	if (this.rt.getRightRoutingTable().isEmpty()) {//core is the last bucket node of the bucket
            	System.out.println("Node " + id + " is the last node of its bucket. Adding node " + newNodeId + " next to it.");
        		long representative = rt.getRepresentative();
        		Vector<Long> lRoutingTable = new Vector<Long>();
        		lRoutingTable.add(this.id);
        		RoutingTable rt = new RoutingTable();
        		rt.setRepresentative(representative);
        		rt.setLeftRoutingTable(lRoutingTable);
                m = new Message(id, newNodeId, new ConnectMessage(rt));
                net.sendMsg(m);
                
                long rightNeighbor = newNodeId;
                Vector<Long> rRoutingTable = new Vector<Long>();
                rRoutingTable.add(rightNeighbor);
                this.rt.setRightRoutingTable(rRoutingTable);
        	}
            else{
            	long rNeighborNode = rt.getRightRoutingTable().get(0);
            	System.out.println("Node " + id + " is a bucket node. Forwarding join message of node " + newNodeId + " to node " + rNeighborNode + ".");
            	msg.setDestinationId(rNeighborNode);
            	net.sendMsg(msg);
            }
        }
        else if (isLeaf()){
        	if (rt.getBucketNode() == RoutingTable.DEF_VAL){ //leaf doesn't have a bucket
            	System.out.println("Node " + id + " is a leaf node with an empty bucket. Adding node " + newNodeId + " to the leaf's bucket.");
        		RoutingTable rt = new RoutingTable();
        		rt.setRepresentative(this.id);
                m = new Message(id, newNodeId, new ConnectMessage(rt));
                net.sendMsg(m);
                
                this.rt.setBucketNode(newNodeId);
        	}
        	else{
            	System.out.println("Node " + id + " is a leaf node. Forwarding node " + newNodeId + " to the leaf's bucket node.");
        		msg.setDestinationId(rt.getBucketNode());
        		net.sendMsg(msg);
        	}
        }
        else { //core is an inner node
        	System.out.println("Node " + id + " is an inner node. Forwarding node " + newNodeId + " to node " + id + "'s left adjacent leaf.");
        	msg.setDestinationId(rt.getLeftAdjacentNode());
        	net.sendMsg(msg);
        }
    }
    
    void connect(Message msg) {
    	ConnectMessage data = (ConnectMessage)msg.getData();
    	RoutingTable rt = data.getRoutingTable();
    	this.rt = rt;
    }
    
    void forwardLookupRequest(Message msg) {
        throw new UnsupportedOperationException("Not supported yet.");
//        long key;
//        long nextHop;
//        LookupRequest data;
//        Message m;
//        
//        //System.out.println(msg);
//        
//        data = (LookupRequest) msg.getData();
//        key = data.getKey();
//        nextHop = key / keySpace + 1;
//        
//        //System.out.println("Id "+Id+" key "+key+" nextHop "+nextHop);
//        
//        if (nextHop == Id) {
//            m = new Message(Id, msg.getSourceId(), new LookupResponse(key, true, msg));
//            Net.sendMsg(m);
//        } 
//        else {
////            if (nextHop > Id) 
////                msg.setDestinationId(Rt.getSuccessor());
////            else
////                msg.setDestinationId(Rt.getPredecessor());
////            Net.sendMsg(msg);
//        }
    }
    boolean isLeaf(){
    	//leaves don't have children
    	return rt.getLeftChild() == RoutingTable.DEF_VAL || rt.getRightChild() == RoutingTable.DEF_VAL;
    }
    boolean isBucketNode(){
    	//bucketNodes have representatives
    	return rt.getRepresentative() != RoutingTable.DEF_VAL;
    }
    int getRtSize() {
        return rt.size();
    }
}
