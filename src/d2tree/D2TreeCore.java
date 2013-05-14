package d2tree;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Vector;

//import D2Tree.ConnectMessage;

import p2p.simulator.message.LookupResponse;
import p2p.simulator.message.Message;
import p2p.simulator.network.Network;

public class D2TreeCore {

	static final int LEFT_CHILD_SIZE  = 1;
	static final int RIGHT_CHILD_SIZE = 2;
	static final int UNEVEN_CHILD     = 3;
    private RoutingTable rt;
    private Network net;
    private long id;
    //private long n;
    //private long k;
    //private float nc; //node criticality
    HashMap<Integer, Long> storedMsgData;

    //TODO keep as singleton for simplicity, convert to Vector of keys later
    //private long key;

    D2TreeCore(long id, long n, long k, Network net) {

        //n = total number of nodes in overlay
        //this.k  	= k;
        //this.n  	= n;
        this.rt 	= new RoutingTable();
        this.net	= net;
        this.id 	= id;
        //this.nc 	= 0.5F;
        storedMsgData	= new HashMap<Integer, Long>();
    }

    /**
     * if core is leaf, then forward to first bucket node if exists, otherwise connect node
     * else if core is an inner node, then forward to nearest leaf
     * else if core is a bucket node, then forward to next bucket node until it's the last bucket node of the bucket
     * else if core is the last bucket node, then connect node
     * **/
    void forwardJoinRequest(Message msg) {
        long newNodeId = msg.getSourceId();
        
        //DEBUG
        System.out.println("Adding node " + newNodeId);
        System.out.println("Forwarded to node " + id);
        
        if (isBucketNode()){ //forward to next bucket node
        	if (this.rt.getRightRoutingTable().isEmpty()) {//core is the last bucket node of the bucket
            	System.out.println("Node " + id + " is the last node of its bucket. Adding node " + newNodeId +
            			" next to it.");
        		long representative = rt.getRepresentative();
        		Vector<Long> lRoutingTable = new Vector<Long>();
        		lRoutingTable.add(this.id);
        		RoutingTable rt = new RoutingTable();
        		rt.setRepresentative(representative);
        		rt.setLeftRoutingTable(lRoutingTable);
                msg = new Message(id, newNodeId, new ConnectMessage(rt));
                
                long rightNeighbor = newNodeId;
                Vector<Long> rRoutingTable = new Vector<Long>();
                rRoutingTable.add(rightNeighbor);
                this.rt.setRightRoutingTable(rRoutingTable);
                Message msg2 = new Message(id, representative, new CheckBalanceRequest());
                net.sendMsg(msg2);
        	}
            else{
            	long rNeighborNode = rt.getRightRoutingTable().get(0);
            	System.out.println("Node " + id + " is a bucket node. Forwarding join message of node " + newNodeId +
            			" to node " + rNeighborNode + ".");
            	msg.setDestinationId(rNeighborNode);
            }
        }
        else if (isLeaf()){
        	if (rt.getBucketNode() == RoutingTable.DEF_VAL){ //leaf doesn't have a bucket
            	System.out.println("Node " + id + " is a leaf node with an empty bucket. Adding node " + newNodeId +
            			" to the leaf's bucket.");
        		RoutingTable rt = new RoutingTable();
        		rt.setRepresentative(this.id);
                msg = new Message(id, newNodeId, new ConnectMessage(rt));
                this.rt.setBucketNode(newNodeId);
        	}
        	else{
            	System.out.println("Node " + id + " is a leaf node. Forwarding node " + newNodeId +
            			" to the leaf's bucket node.");
        		msg.setDestinationId(rt.getBucketNode());
        	}
        }
        else { //core is an inner node
        	System.out.println("Node " + id + " is an inner node. Forwarding node " + newNodeId + " to node " + id +
        			"'s left adjacent leaf.");
        	msg.setDestinationId(rt.getLeftAdjacentNode());
        }
        net.sendMsg(msg);
    }
    
    //find rightmost leaf and mark as endpoint
    //then find leftmost leaf and start redistribution from there until endpoint is reached
    //keep each bucket at around size subtreesize / totalBuckets (total buckets = h^2)
    void forwardBucketRedistributionRequest(){
    }

    void forwardGetSubtreeSizeRequest(Message msg){
    	if (this.isBucketNode()){
    		Vector<Long> rightRT = rt.getRightRoutingTable();
    		if (rightRT.isEmpty()){ //node is last in its bucket
    			GetSubtreeSizeRequest request = (GetSubtreeSizeRequest)msg.getData();
    			long treeSize = request.getSize();
                //msg = new Message(id, msg.getSourceId(), new GetSubtreeSizeResponse(treeSize));
    			msg = new Message(id, rt.getRepresentative(), new GetSubtreeSizeResponse(treeSize, msg.getSourceId()));
                net.sendMsg(msg);
    		}
    		else {
    			long rightNeighbour = rightRT.get(0);
    			GetSubtreeSizeRequest msgData = (GetSubtreeSizeRequest)msg.getData();
    			msgData.incrementSize();
    			msg.setData(msgData);
    			msg.setDestinationId(rightNeighbour);
    			net.sendMsg(msg);
    		}
    	}
    	else if (this.isLeaf()){
    		msg.setDestinationId(rt.getBucketNode());
    		net.sendMsg(msg);
    	}
    	else{
    		msg.setDestinationId(rt.getLeftChild());
    		net.sendMsg(msg);
    		msg.setDestinationId(rt.getRightChild());
    		net.sendMsg(msg);
    	}
    }

    //moves upwards until the initial node is found
    //if this is not the node that initiated the request, then forward to parent
    //else check if parent's subtree needs redistribution
    /***
     * 
	 * first build new data:
	 * if the node is not a leaf, then check if info from both the left and the right subtree exists
     * 
     * and then forward the message to the parent if appropriate
     * @param msg
     */
    void forwardGetSubtreeSizeResponse(Message msg){
    	GetSubtreeSizeResponse data = (GetSubtreeSizeResponse)msg.getData();
    	long destinationID = data.getDestinationID();
    	long givenSize = data.getSize();
    	
    	//determine what the total size of the node's subtree is
    	if (this.isLeaf())
    		data = new GetSubtreeSizeResponse(givenSize, destinationID);
    	else {
			int key = rt.getLeftChild() == msg.getSourceId() ? LEFT_CHILD_SIZE : RIGHT_CHILD_SIZE;
			this.storedMsgData.put(key, data.getSize());
			Long leftSubtreeSize = this.storedMsgData.get(LEFT_CHILD_SIZE);
			Long rightSubtreeSize = this.storedMsgData.get(RIGHT_CHILD_SIZE);
			if (leftSubtreeSize != null && rightSubtreeSize != null){
				data = new GetSubtreeSizeResponse(leftSubtreeSize + rightSubtreeSize, destinationID);
				storedMsgData.clear();
			}
			else return;
    	}
    	msg.setData(data);
		msg.setSourceId(id);
    	if (this.isRoot()) return;
		if (rt.getParent() != destinationID)
			msg.setDestinationId(rt.getParent());
		else
			msg = new Message(id, rt.getParent(), new CheckBalanceRequest());
		net.sendMsg(msg);
    }

    /***
     * get subtree size if not exists (left subtree size + right subtree size)
     * else if subtree is not balanced
     *     forward request to parent and send size data as well
     * else if subtree is balanced, redistribute child
     * 
     * @param msg
     * 
     */
    void forwardCheckBalanceRequest(Message msg) {
    	Long leftSubtreeSize = this.storedMsgData.get(LEFT_CHILD_SIZE);
    	Long rightSubtreeSize = this.storedMsgData.get(RIGHT_CHILD_SIZE);
    	if (leftSubtreeSize == null || rightSubtreeSize == null){
    		if (this.isLeaf()) return;
			if (leftSubtreeSize == null){ //get left subtree size
				msg = new Message(id, rt.getLeftChild(), new GetSubtreeSizeRequest());
				net.sendMsg(msg);
			}
			else this.storedMsgData.put(UNEVEN_CHILD, rt.getLeftChild());
			if (rightSubtreeSize == null){ //get right subtree size
				msg = new Message(id, rt.getRightChild(), new GetSubtreeSizeRequest());
				net.sendMsg(msg);
    		}
			else this.storedMsgData.put(UNEVEN_CHILD, rt.getRightChild());
    	}
		else {
			if (!isBalanced() && !this.isRoot()){
				msg = new Message(id, rt.getParent(), new CheckBalanceRequest());
			}
			else if (isBalanced() && !this.isLeaf()){
				int key = rt.getLeftChild() == UNEVEN_CHILD ? LEFT_CHILD_SIZE : RIGHT_CHILD_SIZE;
				long size = this.storedMsgData.get(key);
				msg = new Message(id, UNEVEN_CHILD, new RedistributeRequest(size));
			}
			net.sendMsg(msg);
		}
    }
    private boolean isBalanced(){
    	if (this.isLeaf())
    		return true;
    	Long leftSubtreeSize = this.storedMsgData.get(LEFT_CHILD_SIZE);
    	Long rightSubtreeSize = this.storedMsgData.get(RIGHT_CHILD_SIZE);
    	Long totalSize = leftSubtreeSize + rightSubtreeSize;
    	float nc = leftSubtreeSize / totalSize;
    	return nc > 0.25 && nc < 0.75;
    }
    void connect(Message msg) {
    	ConnectMessage data = (ConnectMessage)msg.getData();
    	RoutingTable rt = data.getRoutingTable();
    	this.rt = rt;
    }
    void forwardLookupRequest(Message msg) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    boolean isLeaf(){
    	//leaves don't have children
    	return rt.getLeftChild() == RoutingTable.DEF_VAL || rt.getRightChild() == RoutingTable.DEF_VAL;
    }
    boolean isBucketNode(){
    	//bucketNodes have representatives
    	return rt.getRepresentative() != RoutingTable.DEF_VAL;
    }
    boolean isRoot(){
    	return rt.getParent() == RoutingTable.DEF_VAL;
    }
    int getRtSize() {
        return rt.size();
    }
}
