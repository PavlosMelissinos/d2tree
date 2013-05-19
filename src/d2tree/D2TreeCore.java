package d2tree;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Vector;

//import D2Tree.ConnectMessage;

import p2p.simulator.message.LookupResponse;
import p2p.simulator.message.Message;
import p2p.simulator.network.Network;

public class D2TreeCore {

	static final int LEFT_CHILD_SIZE        = 1001;
	static final int RIGHT_CHILD_SIZE       = 1002;
	static final int UNEVEN_CHILD           = 2000;
	static final int SUBTREE_ID             = 3000;

	static final int BUCKET_SIZE            = 4000;
	static final int UNCHECKED_BUCKET_NODES = 5000;
	static final int UNCHECKED_BUCKETS      = 5001;
	static final int MODE                   = 6000;

	static final long MODE_NORMAL         = 0L;
	static final long MODE_CHECK_BALANCE  = 1000L;
	static final long MODE_REDISTRIBUTION = 2000L;
	//static final int TRUE                 = 1;
	//static final int FALSE                = 0;
    private RoutingTable rt;
    private Network net;
    private long id;
    HashMap<Integer, Long> storedMsgData;
    HashMap<Integer, Long> redistData;

    //TODO keep as singleton for simplicity, convert to Vector of keys later
    //private long key;

    D2TreeCore(long id, Network net) {
        this.rt       = new RoutingTable();
        this.net      = net;
        this.id       = id;
        storedMsgData = new HashMap<Integer, Long>();
        redistData    = new HashMap<Integer, Long>();
        storedMsgData.put(MODE, MODE_NORMAL);
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
        
        if (isBucketNode()){
        	if (this.rt.getRightRT().isEmpty()) {//core is the last bucket node of the bucket
            	System.out.println("Node " + id + " is the last node of its bucket. Adding node " + newNodeId +
            			" next to it.");
        		long representative = rt.getRepresentative();
        		Vector<Long> lRoutingTable = new Vector<Long>();
        		lRoutingTable.add(this.id);
        		RoutingTable rt = new RoutingTable();
        		rt.setRepresentative(representative);
        		rt.setLeftRoutingTable(lRoutingTable);
                msg = new Message(id, newNodeId, new ConnectMessage(rt));
                net.sendMsg(msg);
                
                long rightNeighbor = newNodeId;
                Vector<Long> rRoutingTable = new Vector<Long>();
                rRoutingTable.add(rightNeighbor);
                this.rt.setRightRoutingTable(rRoutingTable);
                msg = new Message(id, representative, new CheckBalanceRequest());
                net.sendMsg(msg);
        	}
            else{ //forward to next bucket node
            	long rNeighborNode = rt.getRightRT().get(0);
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
            net.sendMsg(msg);
        }
        else { //core is an inner node
        	System.out.println("Node " + id + " is an inner node. Forwarding node " + newNodeId + " to node " + id +
        			"'s left adjacent leaf.");
        	msg.setDestinationId(rt.getLeftAdjacentNode());
            net.sendMsg(msg);
        }
    }

    /***
     * if node is leaf
     * then get bucket size and check if any nodes need to move
     * else move to left child
     */
    //keep each bucket size at subtreesize / totalBuckets or +1 (total buckets = h^2)
    void forwardBucketRedistributionRequest(Message msg){
    	//if this is an inner node, then forward to left child
    	if (!this.isLeaf() && !this.isBucketNode()){
    		//this.redistData.clear();
    		RedistributeRequest data = (RedistributeRequest)msg.getData();
    		long noofUncheckedBucketNodes = data.getNoofUncheckedBucketNodes();
    		long noofUncheckedBuckets = 2 * data.getNoofUncheckedBuckets();
    		long subtreeID = data.getSubtreeID();
    		msg.setDestinationId(rt.getLeftChild());
    		msg.setData(new RedistributeRequest(noofUncheckedBucketNodes, noofUncheckedBuckets, subtreeID));
    		net.sendMsg(msg);
    	}
    	else if (this.isLeaf()){ 
        	Long bucketSize = this.redistData.get(BUCKET_SIZE);
        	RedistributeRequest data = (RedistributeRequest)msg.getData();
        	if (bucketSize == null){
        		//if it's the first time we visit the leaf, we need to compute the size of the bucket
        		this.redistData.put(SUBTREE_ID, data.getSubtreeID());
        		this.redistData.put(UNCHECKED_BUCKET_NODES, data.getNoofUncheckedBucketNodes());
        		this.redistData.put(UNCHECKED_BUCKETS, data.getNoofUncheckedBucketNodes());
        		msg = new Message(id, rt.getBucketNode(), new GetSubtreeSizeRequest());
        		net.sendMsg(msg);
        	}
        	else{
        		//if we already know the size of the bucket, check 
        		//if any nodes need to move from/to this bucket
            	Long uncheckedBucketNodes = this.redistData.get(UNCHECKED_BUCKET_NODES);
            	long uncheckedBuckets = this.redistData.get(UNCHECKED_BUCKETS);
            	long optimalBucketSize = uncheckedBucketNodes / uncheckedBuckets;
            	long spareNodes = uncheckedBucketNodes % uncheckedBuckets;
            	long diff = bucketSize - optimalBucketSize;
            	if (diff == 0 || (diff == 1 && spareNodes > 0)){//this bucket is ok, so move to the next one (if there is one)
            		uncheckedBuckets--;
            		uncheckedBucketNodes -= bucketSize;
            		if (uncheckedBuckets == 0){
            			//TODO forward ExtendContract Request to the root of the subtree
            			return;
            		}
            		else if (uncheckedBuckets == 0 || uncheckedBucketNodes == 0 || rt.getRightRT().isEmpty()){
            			System.out.println("Something went wrong: ");
            			String rightNeighbor = rt.getRightRT().isEmpty() ? "None" : String.valueOf(rt.getRightRT().get(0));
            			System.out.println("Unchecked buckets: " + uncheckedBuckets + ", Unchecked Bucket Nodes: "
            			+ uncheckedBucketNodes + ", right neighbor: " + rightNeighbor);
            		}
            		long subtreeID = redistData.get(SUBTREE_ID);
            		RedistributeRequest msgData = new RedistributeRequest(uncheckedBucketNodes, uncheckedBuckets, subtreeID);
            		msg = new Message(id, rt.getRightRT().get(0), msgData);
            		net.sendMsg(msg);
            	}
            	else{
            		if (diff < 0){
            			//TODO add nodes from this bucket
            		}
            		else {
            			//TODO remove nodes from this bucket
            		}
            	}
        		
        	}
    	}
    	else throw new UnsupportedOperationException("Unimplemented Method");
    }

    void forwardExtendContractRequest(){
    	
    }
    
    void forwardGetSubtreeSizeRequest(Message msg){
    	if (this.isBucketNode()){
    		Vector<Long> rightRT = rt.getRightRT();
    		if (rightRT.isEmpty()){ //node is last in its bucket
    			GetSubtreeSizeRequest request = (GetSubtreeSizeRequest)msg.getData();
    			long treeSize = request.getSize() + 1;
                //msg = new Message(id, msg.getSourceId(), new GetSubtreeSizeResponse(treeSize));
    			msg = new Message(id, rt.getRepresentative(), new GetSubtreeSizeResponse(treeSize, msg.getSourceId()));
    		}
    		else {
    			long rightNeighbour = rightRT.get(0);
    			GetSubtreeSizeRequest msgData = (GetSubtreeSizeRequest)msg.getData();
    			msgData.incrementSize();
    			msg.setData(msgData);
    			msg.setDestinationId(rightNeighbour);
    		}
			net.sendMsg(msg);
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

    /***
     * 
	 * first build new data:
	 * if the node is not a leaf, then check if info from both the left and the right subtree exists
	 * else find the missing data (by traversing the corresponding subtree and returning the total size of its buckets)
     * 
     * then forward the message to the parent if appropriate
     * @param msg
     */
    void forwardGetSubtreeSizeResponse(Message msg){
    	GetSubtreeSizeResponse data = (GetSubtreeSizeResponse)msg.getData();
    	long givenSize = data.getSize();
    	long destinationID = data.getDestinationID();
    	
		if (this.isLeaf() && id == destinationID && this.storedMsgData.get(MODE) == MODE_REDISTRIBUTION){
			//we've just found out how many nodes this bucket contains so 
			//we now know if we need to add or remove any nodes from here
			this.redistData.put(D2TreeCore.BUCKET_SIZE, givenSize);
			long uncheckedBucketNodes = this.redistData.get(UNCHECKED_BUCKET_NODES);
			long subtreeID = this.redistData.get(SUBTREE_ID);
			RedistributeRequest redistData = new RedistributeRequest(uncheckedBucketNodes, givenSize, subtreeID);
			
			msg = new Message(id, id, redistData);
			//net.sendMsg(msg);
			forwardBucketRedistributionRequest(msg);
			return;
		}
		//determine what the total size of the node's subtree is
		else if (!this.isLeaf()){
			int key = rt.getLeftChild() == msg.getSourceId() ? LEFT_CHILD_SIZE : RIGHT_CHILD_SIZE;
			this.storedMsgData.put(key, givenSize);
			Long leftSubtreeSize = this.storedMsgData.get(LEFT_CHILD_SIZE);
			Long rightSubtreeSize = this.storedMsgData.get(RIGHT_CHILD_SIZE);
			if (leftSubtreeSize != null && rightSubtreeSize != null){
				data = new GetSubtreeSizeResponse(leftSubtreeSize + rightSubtreeSize, destinationID);
				storedMsgData.remove(LEFT_CHILD_SIZE);
				storedMsgData.remove(RIGHT_CHILD_SIZE);
				storedMsgData.remove(UNEVEN_CHILD);
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
		if (this.isLeaf()){
			if (this.isRoot()){
				forwardGetSubtreeSizeRequest(msg);
				net.sendMsg(msg);
			}
			else{
				msg.setDestinationId(rt.getParent());
				net.sendMsg(msg);
			}
		}
		else if (leftSubtreeSize == null || rightSubtreeSize == null){
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
			if (!isBalanced()){
				if (!this.isRoot()){
					msg = new Message(id, rt.getParent(), new CheckBalanceRequest());
					net.sendMsg(msg);
				}
				else {
					//TODO extend or contract
				}
			}
			else if (isBalanced()){
				int key = rt.getLeftChild() == UNEVEN_CHILD ? LEFT_CHILD_SIZE : RIGHT_CHILD_SIZE;
				long totalSubtreeSize = this.storedMsgData.get(key);
				msg = new Message(id, UNEVEN_CHILD, new RedistributeRequest(totalSubtreeSize, 1, UNEVEN_CHILD));
				net.sendMsg(msg);
			}
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
    long getNodeLevel(){
    	return Math.max(rt.getLeftRT().size(), rt.getRightRT().size());
    }
}
