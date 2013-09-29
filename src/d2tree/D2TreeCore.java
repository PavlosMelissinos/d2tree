package d2tree;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Vector;

import p2p.simulator.message.Message;
import p2p.simulator.network.Network;
import d2tree.RoutingTable.Role;
import d2tree.TransferResponse.TransferType;

public class D2TreeCore {

	static final String logDir = "D:\\logs\\";
	//public static HashMap<Long, D2TreeCore> peers;
	public static Vector<D2TreeCore> peers;
    private RoutingTable rt;
    private Network net;
    private long id;
    HashMap<Key, Long> storedMsgData;
    HashMap<Key, Long> redistData;
    private Mode mode;
	
    static enum Key{
		LEFT_CHILD_SIZE,
		RIGHT_CHILD_SIZE,
		UNEVEN_CHILD,
		UNEVEN_SUBTREE_ID,
		BUCKET_SIZE,
		UNCHECKED_BUCKET_NODES,
		UNCHECKED_BUCKETS,
		DEST;
	}
    
    static enum Mode{
    	NORMAL         ("Normal Mode"),
    	CHECK_BALANCE  ("Check Balance Mode"),
    	REDISTRIBUTION ("Redistribution Mode"),
    	TRANSFER       ("Transfer Mode");
    	private String name;
    	Mode(String name){
    		this.name = name;
    	}
    	public String toString(){
    		return name;
    	}
    }

    //TODO keep as singleton for simplicity, convert to Vector of keys later
    //private long key;

    D2TreeCore(long id, Network net) {
        this.rt       = new RoutingTable();
        this.net      = net;
        this.id       = id;
        storedMsgData = new HashMap<Key, Long>();
        redistData    = new HashMap<Key, Long>();
        //storedMsgData.put(MODE, MODE_NORMAL);
        this.mode = Mode.NORMAL;
        //File logDirFile = new File(logDir);
        //if (!logDirFile.exists()) logDirFile.mkdir();
        if (peers == null) peers = new Vector<D2TreeCore>();
        peers.add(this);
    }

    /**
     * if core is leaf, then forward to first bucket node if exists, otherwise connect node
     * else if core is an inner node, then forward to nearest leaf
     * else if core is a bucket node, then forward to next bucket node until it's the last bucket node of the bucket
     * else if core is the last bucket node, then connect node
     * **/
    void forwardJoinRequest(Message msg) {
    	assert msg.getData() instanceof JoinRequest;
        long newNodeId = msg.getSourceId();

        
        if (isBucketNode()){
        	//if (msg.getSourceId() == newNodeId) msg.setSourceId(rt.get(Role.REPRESENTATIVE));
        	if (this.rt.getRightRT().isEmpty()) {//core is the last bucket node of the bucket
        		int msgType = msg.getType();
        		String printMsg = "Node " + newNodeId + " has been added to the bucket of " + rt.get(Role.REPRESENTATIVE) +
                		". Forwarding balance check request to representative with id = " + rt.get(Role.REPRESENTATIVE) + "...";
                ConnectMessage connData = new ConnectMessage(rt.get(Role.REPRESENTATIVE), Role.REPRESENTATIVE, false, newNodeId);
                send(new Message(id, newNodeId, connData));
                connData = new ConnectMessage(id, Role.LEFT_NEIGHBOR, false, newNodeId);
                send(new Message(id, newNodeId, connData));
            	this.print(msg, printMsg, newNodeId);

            	long rightNeighbor = newNodeId;
                //Vector<Long> rightRT = new Vector<Long>();
            	Vector<Long> rightRT = this.rt.getRightRT();
                rightRT.add(rightNeighbor);
                this.rt.setRightRT(rightRT);
                long bucketSize = msg.getHops() + 1;
                msg = new Message(id, rt.get(Role.REPRESENTATIVE), new CheckBalanceRequest(bucketSize, newNodeId));
                send(msg);
                msg = new Message(id, id, new PrintMessage(false, msgType, newNodeId));
                printTree(msg);
        	}
            else{ //forward to next bucket node
            	long rNeighborNode = rt.getRightRT().get(0);
//        		String printMsg = "Node " + id + " is a bucket node. " +
//        				"Forwarding request to right neighbor with id = " + rNeighborNode + "...";
            	//this.print(msg, printMsg, newNodeId);
            	msg.setDestinationId(rNeighborNode);
                send(msg);
            }
        }
        else if (isLeaf()){
        	if (rt.get(Role.BUCKET_NODE) == RoutingTable.DEF_VAL){ //leaf doesn't have a bucket
//        		String printMsg = "Node " + id + " is a bucketless leaf. " +
//        				"Adding " + newNodeId + " as this node's bucket node...";
            	//this.print(msg, printMsg, newNodeId);
                this.rt.set(Role.BUCKET_NODE, newNodeId);
                msg = new Message(id, newNodeId, new ConnectMessage(id, Role.REPRESENTATIVE, false, newNodeId));
                send(msg);
        	}
        	else{
//        		String printMsg = "Node " + id + " is a leaf. " +
//        				"Forwarding request to its bucket node (id = " + rt.get(Role.BUCKET_NODE) + ")...";
            	//this.print(msg, printMsg, newNodeId);
        		msg = new Message(newNodeId, rt.get(Role.BUCKET_NODE), new JoinRequest());
                send(msg);
        	}
        }
        else { //core is an inner node
        	long destination = rt.get(Role.LEFT_A_NODE);
        	if (destination == RoutingTable.DEF_VAL) destination = rt.get(Role.RIGHT_A_NODE);
        	if (destination == RoutingTable.DEF_VAL) destination = rt.get(Role.LEFT_CHILD);
        	if (destination == RoutingTable.DEF_VAL) destination = rt.get(Role.RIGHT_CHILD);
        	if (destination == RoutingTable.DEF_VAL) destination = rt.get(Role.LEFT_NEIGHBOR);
        	if (destination == RoutingTable.DEF_VAL) destination = rt.get(Role.RIGHT_NEIGHBOR);
        	//destination = Math.random() < 0.5 ? rt.get(Role.LEFT_CHILD) : rt.get(Role.RIGHT_CHILD);
//    		String printMsg = "Node " + id + " is an inner node. Forwarding request to " + destination + "...";
        	//this.print(msg, printMsg, newNodeId);
        	msg.setDestinationId(destination);
            send(msg);
        }
    }

    /***
     * if node is leaf
     * then get bucket size and check if any nodes need to move
     * else move to left child
     */
    //keep each bucket size at subtreesize / totalBuckets or +1 (total buckets = h^2)
    void forwardBucketRedistributionRequest(Message msg){
    	assert msg.getData() instanceof RedistributionRequest;
    	RedistributionRequest data = (RedistributionRequest)msg.getData();
    	//if this is an inner node, then forward to right child
    	if (!this.isLeaf() && !this.isBucketNode()){
        	String printMsg = "Node " + id +" is an inner node. Forwarding request to " +
        			rt.get(Role.RIGHT_CHILD) + ".";
        	this.print(msg, printMsg, data.getInitialNode());
    		//this.redistData.clear();
    		long noofUncheckedBucketNodes = data.getNoofUncheckedBucketNodes();
    		long noofUncheckedBuckets = 2 * data.getNoofUncheckedBuckets();
    		long subtreeID = data.getSubtreeID();
    		msg.setDestinationId(rt.get(Role.RIGHT_CHILD));
    		msg.setData(new RedistributionRequest(noofUncheckedBucketNodes, noofUncheckedBuckets, subtreeID, data.getInitialNode()));
    		send(msg);
    		return;
    	}
    	else if (this.isBucketNode()) throw new UnsupportedOperationException("What are you doing here?");

    	
    	Long bucketSize = this.storedMsgData.get(Key.BUCKET_SIZE);
		//if it's the first time we visit the leaf, we need to prepare it for what is to come,
    	//that is compute the size of its bucket and set to "redistribution" mode
    	//if (storedMsgData.get(MODE) == MODE_NORMAL && bucketSize == null){
		if (bucketSize == null){
        	String printMsg = "Node " + id +" is missing bucket info. Computing bucket size...";
        	this.print(msg, printMsg, data.getInitialNode());
			msg = new Message(id, rt.get(Role.BUCKET_NODE), new GetSubtreeSizeRequest(Mode.REDISTRIBUTION, data.getInitialNode()));
			send(msg);
			return;
		}
		//We've reached a leaf, so now we need to figure out which buckets to tamper with
    	Long uncheckedBucketNodes = data.getNoofUncheckedBucketNodes();
    	Long uncheckedBuckets = data.getNoofUncheckedBuckets();
    	if (mode == Mode.NORMAL){
    		redistData.put(Key.UNEVEN_SUBTREE_ID, data.getSubtreeID());
    		redistData.put(Key.UNCHECKED_BUCKET_NODES, uncheckedBucketNodes);
    		redistData.put(Key.UNCHECKED_BUCKETS, uncheckedBuckets);
    		//storedMsgData.put(MODE, MODE_REDISTRIBUTION);
    		if (data.getTransferDest() != RedistributionRequest.DEF_VAL) {
    			//mode = Mode.TRANSFER;
    			redistData.put(Key.DEST, data.getTransferDest());
    		}
    		else{
    			mode = Mode.REDISTRIBUTION;
    			redistData.put(Key.DEST, rt.getLeftRT().get(0));
    		}
    	}
    	else if (mode == Mode.CHECK_BALANCE) return;
    	
		//now that we know the size of the bucket, check 
		//if any nodes need to be transferred from/to this bucket
    	assert !redistData.isEmpty();
    	long optimalBucketSize = uncheckedBucketNodes / uncheckedBuckets;
    	long spareNodes = uncheckedBucketNodes % uncheckedBuckets;
    	long diff = bucketSize - optimalBucketSize;
		long dest = redistData.containsKey(Key.DEST) ? redistData.get(Key.DEST) : -1;
		assert uncheckedBucketNodes != null;
		if (dest == id && uncheckedBucketNodes > 0){
			//if (storedMsgData.get(MODE) == MODE_TRANSFER || diff == 0 || (diff == 1 && spareNodes > 0)){ //this bucket is dest and is ok, so forward to its left neighbor
			if (mode == Mode.TRANSFER || diff == 0 || (diff == 1 && spareNodes > 0)){ //this bucket is dest and is ok, so forward to its left neighbor
				mode = Mode.REDISTRIBUTION;
				if (rt.getLeftRT().isEmpty()){
			    	String printMsg = "Node " + id + " is dest and is ok but doesn't have any neighbors to its left." +
			    			"Going back to pivot bucket with id = " + msg.getSourceId() + "...";
			    	this.print(msg, printMsg, data.getInitialNode());
					redistData.remove(Key.DEST);
					msg = new Message(id, msg.getSourceId(), msg.getData());
					send(msg);
					return;
				}
				msg.setDestinationId(rt.getLeftRT().get(0));
		    	String printMsg = "Node " + id + " is dest. Its bucket is ok (size = " + bucketSize +
		    			"). Forwarding redistribution request to left neighbor with id = " + rt.getLeftRT().get(0) + "...";
		    	this.print(msg, printMsg, data.getInitialNode());
			}
			else{ //this bucket is dest and not ok, so send a response to the source of this message
		    	String printMsg = "Node " + id + " is dest. Its bucket is larger than optimal by " + diff + "(" + bucketSize + " vs "
		    			+ optimalBucketSize + "). Sending redistribution response to node " + msg.getSourceId() + "...";
		    	this.print(msg, printMsg, data.getInitialNode());
				msg = new Message(id, msg.getSourceId(), new RedistributionResponse(bucketSize, data.getInitialNode()));
				//this.storedMsgData.put(MODE, MODE_TRANSFER);
				mode = Mode.TRANSFER;
			}
    		send(msg);
		}
		else if (diff == 0 || (diff == 1 && spareNodes > 0)){//this bucket is ok, so move to the next one (if there is one)
    		long subtreeID = redistData.get(Key.UNEVEN_SUBTREE_ID);
    		this.redistData.clear();
    		//storedMsgData.put(MODE, MODE_NORMAL);
    		mode = Mode.NORMAL;
    		uncheckedBuckets--;
    		uncheckedBucketNodes -= bucketSize;
    		if (uncheckedBuckets == 0){ //redistribution is over so check if the tree needs extension/contraction
    			//TODO forward extend/contract request to the root of the subtree
		    	String printMsg = "The tree is balanced. Doing an extend/contract test...";
		    	this.print(msg, printMsg, data.getInitialNode());
		    	long totalBuckets = new Double(Math.pow(2, rt.getHeight() - 1)).longValue();
		    	long totalBucketNodes = diff == 0 ? optimalBucketSize * totalBuckets : (optimalBucketSize - 1) * totalBuckets;
    			ExtendContractRequest ecData = new ExtendContractRequest(totalBucketNodes, rt.getHeight(), data.getInitialNode());
    			msg = new Message(id, subtreeID, ecData);
    			send(msg);
//		    	CheckBalanceRequest cbData = new CheckBalanceRequest(bucketSize, data.getInitialNode());
//    			msg = new Message(id, id, cbData);
//    			forwardCheckBalanceRequest(msg);
    		}
    		else if (uncheckedBucketNodes == 0 || rt.getLeftRT().isEmpty()){
    			String leftNeighbor = rt.getLeftRT().isEmpty() ? "None" : String.valueOf(rt.getLeftRT().get(0));
		    	String printMsg = "Something went wrong. Unchecked buckets: " + uncheckedBuckets +
		    			", Unchecked Bucket Nodes: " + uncheckedBucketNodes + ", left neighbor: " + leftNeighbor;
		    	this.print(msg, printMsg, data.getInitialNode());
    		}
    		else {
    			RedistributionRequest msgData = new RedistributionRequest(uncheckedBucketNodes, uncheckedBuckets, subtreeID, data.getInitialNode());
    			if (dest == rt.getLeftRT().get(0))
    				msgData.setTransferDest(RedistributionRequest.DEF_VAL);
    			msg = new Message(id, rt.getLeftRT().get(0), msgData);
    			send(msg);
    		}
    	}
    	else { //nodes need to be transferred from/to this node)
    		//storedMsgData.put(MODE, MODE_TRANSFER);
        	String printMsg = "The bucket of node " + id + " is larger than optimal by " + diff + ". Forwarding request to dest = " + dest + ".";
        	this.print(msg, printMsg, data.getInitialNode());
    		mode = Mode.TRANSFER;
			if (dest == RedistributionRequest.DEF_VAL){ //this means we've just started the transfer process
				if (rt.getLeftRT().isEmpty()){
					new Exception().printStackTrace();
				}
				else
					redistData.put(Key.DEST, rt.getLeftRT().get(0));
    			dest = redistData.get(Key.DEST);
			}
			data.setTransferDest(dest);
			msg = new Message(id, dest, data);
			send(msg);
    	}
    }
    void forwardBucketRedistributionResponse(Message msg){
    	assert msg.getData() instanceof RedistributionResponse;
    	long uncheckedBucketNodes = this.redistData.get(Key.UNCHECKED_BUCKET_NODES);
    	long uncheckedBuckets = this.redistData.get(Key.UNCHECKED_BUCKETS);
    	long subtreeID = this.redistData.get(Key.UNEVEN_SUBTREE_ID);
    	long optimalBucketSize = uncheckedBucketNodes / uncheckedBuckets;
    	long bucketSize = this.storedMsgData.get(Key.BUCKET_SIZE);
    	long diff = bucketSize - optimalBucketSize;
    	RedistributionResponse data = (RedistributionResponse)msg.getData();
    	long destDiff = data.getDestSize() - optimalBucketSize;
    	if (diff * destDiff >= 0){ //both this bucket and dest have either more or less nodes
    		//TODO not sure what this does
        	String printMsg = "Node " + id + " and destnode " + msg.getSourceId() + " both have too large (or too small) buckets" +
        			"(" + bucketSize + " and "+ data.getDestSize() + " nodes respectively). Forwarding redistribution request to " + msg.getSourceId() + ".";
        	this.print(msg, printMsg, data.getInitialNode());
    		msg = new Message(id, msg.getSourceId(), new RedistributionRequest(uncheckedBucketNodes, uncheckedBuckets, subtreeID, data.getInitialNode()));
    		send(msg);
    	}
    	else{
    		if (diff > destDiff){ // |pivotBucket| > |destBucket|
    			//move nodes from pivotBucket to destBucket
            	String printMsg = "Node " + id + " has extra nodes that dest node " + msg.getSourceId() + " can use (" + bucketSize + " vs "+ data.getDestSize() + ")" +
            			" Performing transfer from " + id + " to " + msg.getSourceId() + ".";
            	this.print(msg, printMsg, data.getInitialNode());
    			msg = new Message(id, rt.get(Role.BUCKET_NODE), new TransferRequest(msg.getSourceId(), id, true, data.getInitialNode()));
    		}
    		else{ // |pivotBucket| < |destBucket|
            	String printMsg = "Node " + id + " is missing nodes that dest node " + msg.getSourceId() + " has in abundance." +
            			" Performing transfer from " + msg.getSourceId() + " to " + id + ".";
            	this.print(msg, printMsg, data.getInitialNode());
            	msg = new Message(id, msg.getSourceId(), new TransferRequest(msg.getSourceId(), id, true, data.getInitialNode()));
    		}
			send(msg);
    	}
    }
    void forwardTransferRequest(Message msg){
    	assert msg.getData() instanceof TransferRequest;
    	TransferRequest transfData = (TransferRequest)msg.getData();
    	boolean isFirstPass = transfData.isFirstPass(); //is this the first time we run this request
    	long destBucket = transfData.getDestBucket();
    	long pivotBucket = transfData.getPivotBucket();
    	if (this.isLeaf()){
        	String printMsg = "Node " + id + " is a leaf. Forwarding request to bucket node " + rt.get(Role.BUCKET_NODE) + ".";
        	this.print(msg, printMsg, transfData.getInitialNode());
    		msg.setDestinationId(rt.get(Role.BUCKET_NODE));
    		send(msg);
    		return;
    	}
    	//this is a bucket node
		if (!rt.getRightRT().isEmpty() && rt.get(Role.REPRESENTATIVE) != pivotBucket){
			//we are at the dest bucket
			//forward request to right neighbor until we reach the last node in the bucket
        	String printMsg = "Node " + id + " is a bucket node of dest. Forwarding request to right neighbor " + rt.getRightRT().get(0) + ".";
        	this.print(msg, printMsg, transfData.getInitialNode());
    		msg.setDestinationId(rt.getRightRT().get(0));
    		send(msg);
    		return;
		}
		if (rt.getRightRT().isEmpty()){
			//we are at the dest bucket
			//forward request to right neighbor until we reach the last node in the bucket
        	String printMsg = "Node " + id + " is the last bucket node of dest. Initiating node transfer...";
        	this.print(msg, printMsg, transfData.getInitialNode());
		}
		
		//this runs either on the last node of the dest bucket
		//or the first node of the pivot bucket
		if (isFirstPass){
        	String printMsg = "Performing first-pass transfer ";
			if (rt.get(Role.REPRESENTATIVE) != pivotBucket){
				//move this node from the dest bucket to pivot (as first pass)
				printMsg += "from " + destBucket + " to " + pivotBucket;
	        	this.print(msg, printMsg, transfData.getInitialNode());
				//remove the link from the left neighbor to this node
				msg = new Message(id, rt.getLeftRT().get(0), new DisconnectMessage(id, Role.RIGHT_NEIGHBOR, transfData.getInitialNode()));
				send(msg);
				
				//remove the link from this node to its left neighbor
				rt.setLeftRT(new Vector<Long>());
				
				//send message to the pivot bucket with the new node
				transfData = new TransferRequest(destBucket, pivotBucket, false, transfData.getInitialNode());
				msg = new Message(id, pivotBucket, transfData);
				send(msg);
			}
			else{
				//move this node from the pivot bucket to dest (as first pass)
				printMsg += "from " + pivotBucket + " to " + destBucket;
	        	this.print(msg, printMsg, transfData.getInitialNode());
				//remove the link from the right neighbor to this node
				msg = new Message(id, rt.getRightRT().get(0), new DisconnectMessage(id, Role.LEFT_NEIGHBOR, transfData.getInitialNode()));
				send(msg);
				
				//remove the link from this node to its left neighbor
				rt.setRightRT(new Vector<Long>());
				
				//send message to the dest bucket with the new node
				transfData = new TransferRequest(destBucket, pivotBucket, false, transfData.getInitialNode());
				msg = new Message(id, destBucket, transfData);
				send(msg);
			}
		}
		else{ //second pass
        	String printMsg = "Performing second-pass transfer of " + msg.getSourceId() + " from ";
			if (rt.get(Role.REPRESENTATIVE) != pivotBucket){
				//move pivotNode from the pivot bucket to dest (as second pass)
				printMsg += pivotBucket + " to " + destBucket;
	        	this.print(msg, printMsg, transfData.getInitialNode());
				
				long pivotNode = msg.getSourceId();
				
				//add a link from pivotNode to the representative of destNode
//				printMsg = "Setting " + rt.get(Role.REPRESENTATIVE) + " as the representative of node " + pivotNode + "...";
//	        	this.print(msg, printMsg, transfData.getInitialNode());
				ConnectMessage connData = new ConnectMessage(rt.get(Role.REPRESENTATIVE), Role.REPRESENTATIVE, true, transfData.getInitialNode());
				msg = new Message(id, pivotNode, connData);
				send(msg);				
				
				//add a link from pivotNode to destNode
//				printMsg = "Setting " + id + " as the left neighbor of node " + pivotNode + "...";
//	        	this.print(msg, printMsg, transfData.getInitialNode());
				connData = new ConnectMessage(id, Role.LEFT_NEIGHBOR, true, transfData.getInitialNode());
				msg = new Message(id, pivotNode, connData);
				send(msg);
				
				//add a link from destNode to pivotNode
//				printMsg = "Setting " + pivotNode + " as the right neighbor of node " + id + "...";
//	        	this.print(msg, printMsg, transfData.getInitialNode());
				connData = new ConnectMessage(pivotNode, Role.RIGHT_NEIGHBOR, true, transfData.getInitialNode());
				msg = new Message(id, id, connData);
				connect(msg);
				
				TransferResponse respData = new TransferResponse(TransferType.NODE_REMOVED, pivotBucket, transfData.getInitialNode());
				msg = new Message(id, pivotBucket, respData);
				send(msg);

				respData = new TransferResponse(TransferType.NODE_ADDED, pivotBucket, transfData.getInitialNode());
				msg = new Message(id, destBucket, respData);
				send(msg);
				
				printMsg = "Successfully moved " + pivotNode + " next to " + id + "...";
			}
			else{
				//move destNode from the dest bucket to pivot (as second pass)
				printMsg += destBucket + " to " + pivotBucket;
	        	this.print(msg, printMsg, transfData.getInitialNode());
				
				long destNode = msg.getSourceId();
				
				//add a link from destNode to pivotNode's representative
//				printMsg = "Setting " + rt.get(Role.REPRESENTATIVE) + " as the representative of node " + destNode + "...";
//	        	this.print(msg, printMsg, transfData.getInitialNode());
				ConnectMessage connData = new ConnectMessage(rt.get(Role.REPRESENTATIVE), Role.REPRESENTATIVE, true, transfData.getInitialNode());
				msg = new Message(id, destNode, connData);
				send(msg);
				
				//add a link from pivotNode's representative to destNode
//				printMsg = "Setting " + destNode + " as the bucket node of node " + rt.get(Role.REPRESENTATIVE) + "...";
//	        	this.print(msg, printMsg, transfData.getInitialNode());
				connData = new ConnectMessage(destNode, Role.BUCKET_NODE, true, transfData.getInitialNode());
				msg = new Message(id, rt.get(Role.REPRESENTATIVE), connData);
				send(msg);
				
				//add a link from pivotNode to destNode
//				printMsg = "Setting " + destNode + " as the left neighbor of node " + id + "...";
//	        	this.print(msg, printMsg, transfData.getInitialNode());
				connData = new ConnectMessage(destNode, Role.LEFT_NEIGHBOR, true, transfData.getInitialNode());
				msg = new Message(id, id, connData);
				connect(msg);
				
				//add a link from destNode to pivotNode
//				printMsg = "Setting " + id + " as the right neighbor of node " + destNode + "...";
//	        	this.print(msg, printMsg, transfData.getInitialNode());
				connData = new ConnectMessage(id, Role.RIGHT_NEIGHBOR, true, transfData.getInitialNode());
				msg = new Message(id, destNode, connData);
				send(msg);

				TransferResponse respData = new TransferResponse(TransferType.NODE_ADDED, pivotBucket, transfData.getInitialNode());
				msg = new Message(id, pivotBucket, respData);
				send(msg);
				
				respData = new TransferResponse(TransferType.NODE_REMOVED, pivotBucket, transfData.getInitialNode());
				msg = new Message(id, destBucket, respData);
				send(msg);
				printMsg = "Successfully moved " + destNode + " next to " + id + "...";
			}
			printMsg += " Redistribution has been successful. THE END";
			print(msg, printMsg, transfData.getInitialNode());
			PrintMessage printData = new PrintMessage(false, msg.getType(), transfData.getInitialNode());
			//printTree(new Message(id, rt.get(Role.REPRESENTATIVE), printData));
			printTree(new Message(id, id, printData));
		}
    }
    
    void forwardTransferResponse(Message msg){
    	assert msg.getData() instanceof TransferResponse;
    	TransferResponse data = (TransferResponse)msg.getData();
    	if (this.isLeaf()){
    		TransferType transfType = data.getTransferType();
    		long pivotBucket = data.getPivotBucket();
    		long bucketSize = storedMsgData.get(Key.BUCKET_SIZE);
    		bucketSize = transfType == TransferType.NODE_ADDED ? bucketSize + 1 : bucketSize - 1;
    		storedMsgData.put(Key.BUCKET_SIZE, bucketSize);
    		
    		if (pivotBucket == id) return;
    		
    		boolean isDestBucket = redistData.get(Key.DEST) == id;
    		if (isDestBucket){
				String printMsg = "Node " + id + " is dest bucket. Forwarding redistribution response " +
						"to pivot bucket with id = " + pivotBucket + "...";
				print(msg, printMsg, data.getInitialNode());
    			RedistributionResponse rData = new RedistributionResponse(bucketSize, data.getInitialNode());
    			msg = new Message(id, pivotBucket, rData);
    			send(msg);
    		}
    	}
//    	long unevenSubtreeID = redistData.get(D2TreeCore.UNEVEN_SUBTREE_ID);
//    	if (unevenSubtreeID != id){
//    		msg.setDestinationId(unevenSubtreeID);
//    		send(msg);
//    	}
//    	if (this.isRoot()){
//    		TransferResponse data = (TransferResponse)msg.getData();
//    		int bucketSize = data.;
//    		if (subtreeSize > )
//    		msg = new Message(id, id, new ExtendContractRequest(D2TreeMessageT.EXTEND_REQ));
//    	}
    }

    void forwardExtendContractRequest(Message msg){
    	assert msg.getData() instanceof ExtendContractRequest;
    	if (!this.isRoot()){//We only extend and contract if the root was uneven.
    		if (!this.isBucketNode()){
    			msg.setDestinationId(rt.get(Role.PARENT));
    			send(msg);
    		}
			return;
    	}
    	
    	ExtendContractRequest data = (ExtendContractRequest)msg.getData();
    	
    	long treeHeight = data.getHeight();
    	long totalBucketNodes = data.getTotalBucketNodes();
    	long totalBuckets = new Double(Math.pow(2, data.getHeight() - 1)).longValue();
    	double averageBucketSize = (double)totalBucketNodes / (double)totalBuckets;
    	double factor = 2.0;
    	double offset = 4.0;
    	boolean shouldExtend   = factor * treeHeight + offset <= averageBucketSize;
    	boolean shouldContract = treeHeight >= averageBucketSize * factor + offset;
		String printMsg = "Tree height is " + treeHeight + " and average bucket size is " + averageBucketSize +
				" (bucket nodes are " + totalBucketNodes + " and there are " + 
				totalBuckets + " unchecked buckets). ";
    	if (shouldExtend){
    		printMsg += "Initiating tree extension.";
    		print(msg, printMsg, data.getInitialNode());
    		//ExtendRequest eData = new ExtendRequest(Math.round(optimalBucketSize), (long)averageBucketSize, data.getInitialNode());
    		ExtendRequest eData = new ExtendRequest((long)averageBucketSize, true, data.getInitialNode());
    		msg = new Message(id, id, eData);
        	send(msg);
    	}
    	else if (shouldContract && !this.isLeaf()){
    		printMsg += "Initiating tree contraction.";
        	print(msg, printMsg, data.getInitialNode());
        	//ContractRequest cData = new ContractRequest(Math.round(optimalBucketSize), data.getInitialNode());
        	ContractRequest cData = new ContractRequest((long)averageBucketSize, data.getInitialNode());
    		msg = new Message(id, id, cData);
        	send(msg);
    	}
    	else if (shouldContract && this.isLeaf()){
    		printMsg += "Tree is already at minimum height. Can't contract any more.";
        	print(msg, printMsg, data.getInitialNode());
    		
    	}
    	else {
    		printMsg += "No action needed.";
    		print(msg, printMsg, data.getInitialNode());
    	}
    }
    void forwardExtendRequest(Message msg){
    	assert msg.getData() instanceof ExtendRequest;
    	ExtendRequest data = (ExtendRequest)msg.getData();
		long initialNode = data.getInitialNode();

    	//travel to leaves of the tree, if not already there
    	if (!this.isBucketNode()){
    		if (this.isLeaf()){ //forward request to the bucket node
    			Long bucketSize = this.storedMsgData.get(Key.BUCKET_SIZE);
        		String printMsg = "Node " + id + " is a leaf with size = " + bucketSize + ". Forwarding request to bucket node with id = " + rt.get(Role.BUCKET_NODE) + "...";
            	this.print(msg, printMsg, initialNode);
            	data = new ExtendRequest(bucketSize, true, initialNode);
        		msg = new Message(msg.getSourceId(), rt.get(Role.BUCKET_NODE), data); //reset hop count
        		send(msg);
    		}
    		else{ //forward request to the children
        		String printMsg = "Node " + id + " is an inner node. Forwarding request to children with id = "
        				+ rt.get(Role.LEFT_CHILD) + " and " + rt.get(Role.RIGHT_CHILD) + " respectively...";
            	this.print(msg, printMsg, data.getInitialNode());
    			//msg.setDestinationId(rt.get(Role.LEFT_CHILD));
            	Message msg1 = new Message(msg.getSourceId(), rt.get(Role.LEFT_CHILD), (ExtendRequest)msg.getData());
    			send(msg1);

    			//msg.setDestinationId(rt.get(Role.RIGHT_CHILD));
    			Message msg2 = new Message(msg.getSourceId(), rt.get(Role.RIGHT_CHILD), (ExtendRequest)msg.getData());
    			send(msg2);
    		}
    		return;
    	}
    	
    	// this is a bucket node
    	long oldOptimalBucketSize = data.getOldOptimalBucketSize();
    	long optimalBucketSize = (oldOptimalBucketSize - 1) / 2; //trick, accounts for odd vs even optimal sizes
    	//long optimalBucketSize = data.getOptimalBucketSize();
    	int counter = msg.getHops();
    	if (rt.getLeftRT().isEmpty()){//this is the first node of the bucket, make it a left leaf
    		String printMsg = "Node " + id + " is the first node of bucket " + rt.get(Role.REPRESENTATIVE) + " (index = "+counter+"). Making it a left leaf...";
    		rt.print(new PrintWriter(System.err));
        	this.print(msg, printMsg, data.getInitialNode());
    		bucketNodeToLeftLeaf(data);
    	}
    	else if (counter == optimalBucketSize + 1 && data.buildsLeftLeaf()){ //the left bucket is full, make this a right leaf
    		//forward extend response to the old leaf
    		long leftLeaf = msg.getSourceId();
    		long rightLeaf = id;
    		long oldLeaf = rt.get(Role.REPRESENTATIVE);
    		
    		//the left bucket is full, forward a new extend request to the new (left) leaf

    		String printMsg = "Node " + id + " is the middle node of bucket "+rt.get(Role.REPRESENTATIVE)+" (index = "+counter+"). Left leaf is the node with id = " +
    				leftLeaf + " (bucket size = " + (counter - 1) + ") and the old leaf has id = " + oldLeaf + ". Making " + id + " a right leaf...";
        	this.print(msg, printMsg, data.getInitialNode());

        	ExtendResponse exData = new ExtendResponse(0, leftLeaf, rightLeaf, data.getInitialNode());
    		msg = new Message(id, oldLeaf, exData);
    		send(msg);
    		
    		bucketNodeToRightLeaf(data);
    	}
    	else{
    		long oldLeaf = rt.get(Role.REPRESENTATIVE);
    		long newLeaf = msg.getSourceId();
    		rt.set(Role.REPRESENTATIVE, newLeaf);
    		if (rt.getLeftRT().get(0) == newLeaf) //this is the first node of the new bucket
    			rt.setLeftRT(new Vector<Long>()); //disconnect from newLeaf
    		
    		if (!rt.getRightRT().isEmpty()){
        		String printMsg = "Node " + id + " is the " + counter + "th node of ex-bucket "+oldLeaf+". Forwarding request to its right neighbor with id = " + rt.getRightRT().get(0) + "...";
            	this.print(msg, printMsg, data.getInitialNode());
	    		//forward to next bucket node
	    		msg.setDestinationId(rt.getRightRT().get(0));
	    		send(msg);
    		}
    		else{
        		String printMsg = "Node " + id + " is the last node of ex-bucket " + oldLeaf+". Routing table has been built.";
            	this.print(msg, printMsg, data.getInitialNode());
            	msg = new Message(id, id, new PrintMessage(false, msg.getType(), data.getInitialNode()));
            	printTree(msg);
    		}
    	}
    }

	void bucketNodeToLeftLeaf(ExtendRequest data){
		long oldLeaf = rt.get(Role.REPRESENTATIVE);
		long rightNeighbor = rt.getRightRT().get(0);
		
		//forward the request to the right neighbor
		send(new Message(id, rightNeighbor, data));
		
		//set this as the left child of the old leaf
		ConnectMessage connData = new ConnectMessage(this.id, Role.LEFT_CHILD, true, data.getInitialNode());
		send(new Message(id, oldLeaf, connData));

//		//set this as the left adjacent node of the old leaf
//		connData = new ConnectMessage(this.id, Role.LEFT_A_NODE, data.getInitialNode());
//		send(new Message(id, oldLeaf, connData));

		//disconnect the bucket node of the old leaf
		DisconnectMessage discData = new DisconnectMessage(this.id, Role.BUCKET_NODE, data.getInitialNode());
		send(new Message(id, oldLeaf, discData));
		
		//TODO newLeaf.leftAdjacentNode <== oldLeaf.leftAdjacentNode
		//TODO oldLeaf.leftAdjacentNode.rightAdjacentNode <== newLeaf.leftAdjacentNode
		
		rt.set(Role.PARENT, oldLeaf); //set the old leaf as the parent of this node
		rt.set(Role.RIGHT_A_NODE, oldLeaf); //set the old leaf as the left adjacent node of this node
		rt.set(Role.REPRESENTATIVE, RoutingTable.DEF_VAL); //disconnect the representative
		rt.set(Role.BUCKET_NODE, rightNeighbor); //set the right neighbor as the bucketNode of this node
		//empty routing tables
		rt.setRightRT(new Vector<Long>());
		rt.setLeftRT(new Vector<Long>());
		
		//TODO make new routing tables
		String printMsg = "Bucket node " + id + " successfully turned into a left leaf...";
    	this.print(new Message(id, id, data), printMsg, data.getInitialNode());
    }
    void bucketNodeToRightLeaf(ExtendRequest data){
		long oldLeaf = rt.get(Role.REPRESENTATIVE);
		long rightNeighbor = rt.getRightRT().get(0);
		
		//forward the request to the right neighbor
		data = new ExtendRequest(data.getOldOptimalBucketSize(), false, data.getInitialNode());
		send(new Message(id, rightNeighbor, data));
		
		//set this as the right child of the old leaf
		ConnectMessage connData = new ConnectMessage(this.id, Role.RIGHT_CHILD, true, data.getInitialNode());
		send(new Message(id, oldLeaf, connData));

//		//set this as the right adjacent node of the old leaf
//		connData = new ConnectMessage(this.id, Role.RIGHT_A_NODE, data.getInitialNode());
//		send(new Message(id, oldLeaf, connData));
		
		//disconnect from left neighbor as right neighbor
		DisconnectMessage discData = new DisconnectMessage(this.id, Role.RIGHT_NEIGHBOR, data.getInitialNode());
		send(new Message(id, rt.getLeftRT().get(0), discData));
		
		//TODO newLeaf.rightAdjacentNode <== oldLeaf.rightAdjacentNode
		//TODO oldLeaf.rightAdjacentNode.leftAdjacentNode <== newLeaf.rightAdjacentNode
		
		rt.set(Role.PARENT, oldLeaf); //set the old leaf as the parent of this node
		rt.set(Role.LEFT_A_NODE, oldLeaf); //set the old leaf as the left adjacent node of this node
		rt.set(Role.REPRESENTATIVE, RoutingTable.DEF_VAL); //disconnect the representative
		rt.set(Role.BUCKET_NODE, rightNeighbor); //set the right neighbor as the bucketNode of this node
		//empty routing tables
		rt.setRightRT(new Vector<Long>());
		rt.setLeftRT(new Vector<Long>());
		
		//TODO make new routing tables
		String printMsg = "Bucket node " + id + " successfully turned into a right leaf...";
    	this.print(new Message(id, id, data), printMsg, data.getInitialNode());
    	
    }
    void forwardExtendResponse(Message msg){
    	assert msg.getData() instanceof ExtendResponse;
    	ExtendResponse data = (ExtendResponse)msg.getData();
    	int index = data.getIndex();
    	long lChild0 = data.getLeftChild();
    	long rChild0 = data.getRightChild();
    	if (index == 0){
    		if (rt.get(Role.LEFT_A_NODE) != RoutingTable.DEF_VAL && rt.get(Role.LEFT_A_NODE) != lChild0){ //add a link from left adjacent to left child
    			ConnectMessage connData = new ConnectMessage(lChild0, Role.RIGHT_A_NODE, true, data.getInitialNode());
    			msg = new Message(id, rt.get(Role.LEFT_A_NODE), connData);
    			send(msg);
    		}
			
    		if (lChild0 != RoutingTable.DEF_VAL && rt.get(Role.LEFT_A_NODE) != lChild0){ //add a link from left child to left adjacent
    			ConnectMessage connData = new ConnectMessage(rt.get(Role.LEFT_A_NODE), Role.LEFT_A_NODE, true, data.getInitialNode());
				msg = new Message(id, lChild0, connData);
				send(msg);
    		}
	    	
    		if (rt.get(Role.RIGHT_A_NODE) != RoutingTable.DEF_VAL && rt.get(Role.RIGHT_A_NODE) != rChild0){ //add a link from right adjacent to right child
    			ConnectMessage connData = new ConnectMessage(rChild0, Role.LEFT_A_NODE, true, data.getInitialNode());
				msg = new Message(id, rt.get(Role.RIGHT_A_NODE), connData);
				send(msg);
    		}
			
    		if (rChild0 != RoutingTable.DEF_VAL && rt.get(Role.RIGHT_A_NODE) != rChild0){ //add a link from right child to right adjacent
    			ConnectMessage connData = new ConnectMessage(rt.get(Role.RIGHT_A_NODE), Role.RIGHT_A_NODE, true, data.getInitialNode());
				msg = new Message(id, rChild0, connData);
				send(msg);
    		}

			if (rChild0 != RoutingTable.DEF_VAL){ //add a link from right child to left child
    			ConnectMessage connData = new ConnectMessage(lChild0, Role.LEFT_NEIGHBOR, true, data.getInitialNode());
    			msg = new Message(id, rChild0, connData);
    			send(msg);
			}
			
			if (lChild0 != RoutingTable.DEF_VAL){ //add a link from left child to right child
				ConnectMessage connData = new ConnectMessage(rChild0, Role.RIGHT_NEIGHBOR, true, data.getInitialNode());
    			msg = new Message(id, lChild0, connData);
    			send(msg);
			}
			
			rt.set(Role.LEFT_A_NODE, lChild0);
			rt.set(Role.RIGHT_A_NODE, rChild0);
			
			for (int i = 0; i < rt.getLeftRT().size(); i++){
				long node = rt.getLeftRT().get(i);
				data = new ExtendResponse(-i - 1, lChild0, rChild0, data.getInitialNode());
				msg = new Message(msg.getSourceId(), node, data);
				send(msg);
			}
			for (int i = 0; i < rt.getRightRT().size(); i++){
				long node = rt.getRightRT().get(i);
				data = new ExtendResponse(i + 1, lChild0, rChild0, data.getInitialNode());
				msg = new Message(msg.getSourceId(), node, data);
				send(msg);
			}
			//TODO forward extend requests to the new leaves if their size is not optimal
			//ExtendRequest exData = new ExtendRequest(ecData.get(D2TreeCore.), );
    	}
    	else{
    		long lChildi = rt.get(Role.LEFT_CHILD);
    		long rChildi = rt.get(Role.RIGHT_CHILD);
    		if (index < 0 ){ //old leaf's left RT
        		if (index == -1){ //this is the left neighbor of the old leaf
        			
        			if (rChildi != RoutingTable.DEF_VAL){ //add a link from this node's right child to the original left child
	        			ConnectMessage connData = new ConnectMessage(lChild0, Role.RIGHT_NEIGHBOR, true, data.getInitialNode());
	        			msg = new Message(id, rChildi, connData);
	        			send(msg);
        			}
        			
        			if (lChild0 != RoutingTable.DEF_VAL){ //add a link from the original left child to this node's right child
        				ConnectMessage connData = new ConnectMessage(rChildi, Role.LEFT_NEIGHBOR, true, data.getInitialNode());
	        			msg = new Message(id, lChild0, connData);
	        			send(msg);
        			}
        		}
    			
        		if (lChild0 != RoutingTable.DEF_VAL){ //add a link from the original left child to this node's left child
        			ConnectMessage connData = new ConnectMessage(lChildi, Role.LEFT_RT, -index, true, data.getInitialNode());
	    			msg = new Message(id, lChild0, connData);
	    			send(msg);
        		}
    			
        		if (rChild0 != RoutingTable.DEF_VAL){ //add a link from the original right child to this node's right child
        			ConnectMessage connData = new ConnectMessage(rChildi, Role.LEFT_RT, -index, true, data.getInitialNode());
	    			msg = new Message(id, rChild0, connData);
	    			send(msg);
        		}

        		if (lChildi != RoutingTable.DEF_VAL){ //add a link from this node's left child to the original left child
	    			ConnectMessage connData = new ConnectMessage(lChild0, Role.RIGHT_RT, -index, true, data.getInitialNode());
	    			msg = new Message(id, lChildi, connData);
	    			send(msg);
        		}
        		
        		if (rChildi != RoutingTable.DEF_VAL){ //add a link from this node's right child to the original right child
        			ConnectMessage connData = new ConnectMessage(rChild0, Role.RIGHT_RT, -index, true, data.getInitialNode());
	    			msg = new Message(id, rChildi, connData);
	    			send(msg);
        		}
    		}
    		else { //old leaf's right RT
    			if (index == 1){ //this is the right neighbor of the old leaf
        			
    				if (rChildi != RoutingTable.DEF_VAL){ //add a link from this node's left child to the original right child
	        			ConnectMessage connData = new ConnectMessage(rChild0, Role.LEFT_NEIGHBOR, true, data.getInitialNode());
	        			send(new Message(id, lChildi, connData));
    				}
        			
        			if (rChild0 != RoutingTable.DEF_VAL){ //add a link from the original right child to this node's left child
        				ConnectMessage connData = new ConnectMessage(lChildi, Role.RIGHT_NEIGHBOR, true, data.getInitialNode());
        				send(new Message(id, rChild0, connData));
        			}
        			
        		}
    			if (lChildi != RoutingTable.DEF_VAL){ //add a link from this node's left child to the original left child
	    			ConnectMessage connData = new ConnectMessage(lChild0, Role.LEFT_RT, index, true, data.getInitialNode());
	    			msg = new Message(id, lChildi, connData);
	    			send(msg);
    			}

    			if (rChildi != RoutingTable.DEF_VAL){ //add a link from this node's right child to the original right child
	    			ConnectMessage connData = new ConnectMessage(rChild0, Role.LEFT_RT, index, true, data.getInitialNode());
	    			msg = new Message(id, rChildi, connData);
	    			send(msg);
    			}
    			
    			if (lChild0 != RoutingTable.DEF_VAL){ //add a link from the original left child to this node's left child
	    			ConnectMessage connData = new ConnectMessage(lChildi, Role.RIGHT_RT, index, true, data.getInitialNode());
	    			msg = new Message(id, lChild0, connData);
	    			send(msg);
    			}
    			
    			if (rChild0 != RoutingTable.DEF_VAL){ //add a link from the original right child to this node's right child
	    			ConnectMessage connData = new ConnectMessage(rChildi, Role.RIGHT_RT, index, true, data.getInitialNode());
	    			msg = new Message(id, rChild0, connData);
	    			send(msg);
    			}
    		}
    	}
    }
    void forwardContractRequest(Message msg){
    	assert msg.getData() instanceof ContractRequest;
		//TODO contract
    }
    void forwardGetSubtreeSizeRequest(Message msg){
		GetSubtreeSizeRequest msgData = (GetSubtreeSizeRequest)msg.getData();
		Mode msgMode = msgData.getMode();
    	if (this.isBucketNode()){
    		Vector<Long> rightRT = rt.getRightRT();
			//TODO Fix bug (forwardGetSubtreeSizeRequest is seemingly called twice for nodes that are last in their buckets)
			msgData.incrementSize();
			msg.setData(msgData);
    		if (rightRT.isEmpty()){ //node is last in its bucket
    			//long bucketSize = msg.getHops() - 1;
    			long bucketSize = msgData.getSize();
        		String printMsg = "This node is the last in its bucket (size = " + bucketSize +
    					"). Sending response to its representative, with id " + rt.get(Role.REPRESENTATIVE) + ".";
		    	this.print(msg, printMsg, msgData.getInitialNode());
    			msg = new Message(id, rt.get(Role.REPRESENTATIVE),
    					new GetSubtreeSizeResponse(msgMode, bucketSize, msg.getSourceId(), msgData.getInitialNode()));
    			send(msg);
    		}
    		else {
//		    	String printMsg = "Node " + this.id +
//		    			". Forwarding request to its right neighbor, with id " + rt.getRightRT().get(0) +
//		    			". (size = " + msgData.getSize() + ")";
		    	//this.print(msg, printMsg);
    			long rightNeighbour = rightRT.get(0);
    			msg.setDestinationId(rightNeighbour);
    			send(msg);
    		}
    	}
    	else if (this.isLeaf()){
    		Long bucketSize = storedMsgData.get(Key.BUCKET_SIZE); 
    		if (bucketSize == null)
    		{
    			String printMsg = "This is a leaf. Forwarding to bucket node with id "
    				+ rt.get(Role.BUCKET_NODE) + ".";
	    		this.print(msg, printMsg, msgData.getInitialNode());
    			msg.setDestinationId(rt.get(Role.BUCKET_NODE));
    		}
    		else{
				msg = new Message(id, rt.get(Role.PARENT),
						new GetSubtreeSizeResponse(msgMode, bucketSize, msg.getSourceId(), msgData.getInitialNode()));
    			if (rt.get(Role.PARENT) == RoutingTable.DEF_VAL) msg.setDestinationId(id);
    			String printMsg = "This is a leaf with a bucket size of = " + bucketSize +
    				". Forwarding response to node with id = " + msg.getDestinationId() + ".";
    			this.print(msg, printMsg, msgData.getInitialNode());
    		}
			send(msg);
    	}
    	else{
	    	String printMsg = "This is an inner node. Forwarding request to its children with ids "
	    			+ rt.get(Role.LEFT_CHILD) + " and " + rt.get(Role.RIGHT_CHILD) + ".";
	    	this.print(msg, printMsg, msgData.getInitialNode());
    		msg.setDestinationId(rt.get(Role.LEFT_CHILD));
    		send(msg);
    		msg.setDestinationId(rt.get(Role.RIGHT_CHILD));
    		send(msg);
    	}
    }

    /***
     * 
	 * build new data first:
	 * if the node is not a leaf, then check if info from both the left and the right subtree exists
	 * else find the missing data (by traversing the corresponding subtree and returning the total size of its buckets)
	 * 
     * 
     * then forward the message to the parent if appropriate
     * @param msg
     */
    void forwardGetSubtreeSizeResponse(Message msg){
    	assert msg.getData() instanceof GetSubtreeSizeResponse;
    	GetSubtreeSizeResponse data = (GetSubtreeSizeResponse)msg.getData();
    	Mode msgMode = data.getMode();
    	long givenSize = data.getSize();
    	long destinationID = data.getDestinationID();

		//determine what the total size of the node's subtree is
    	//if this is leaf, data.getSize() gives us the size of its bucket, which coincidentally is the size of its subtree
    	if (!this.isLeaf()){
    		String printMsg = "This is not a leaf. Destination ID = " + destinationID + ". (bucket size = " +
    			data.getSize() + ") Mode = " + mode + " vs MsgMode = " + msgMode;
        	this.print(msg, printMsg, data.getInitialNode());
			Key key = rt.get(Role.LEFT_CHILD) == msg.getSourceId() ? Key.LEFT_CHILD_SIZE : Key.RIGHT_CHILD_SIZE;
			this.storedMsgData.put(key, givenSize);
			Long leftSubtreeSize = this.storedMsgData.get(Key.LEFT_CHILD_SIZE);
			Long rightSubtreeSize = this.storedMsgData.get(Key.RIGHT_CHILD_SIZE);
			printMsg = "Incomplete subtree data ("+ leftSubtreeSize + " vs " + rightSubtreeSize + "). ";
			if (leftSubtreeSize == null || rightSubtreeSize == null){
				if (leftSubtreeSize == null){
					printMsg += "Computing left subtree size (id = " + rt.get(Role.LEFT_CHILD) + ")...";
		        	this.print(msg, printMsg, data.getInitialNode());
		    		msg = new Message(destinationID, rt.get(Role.LEFT_CHILD), new GetSubtreeSizeRequest(msgMode, data.getInitialNode()));
		    		send(msg);
				}
				if (rightSubtreeSize == null) {
		    		printMsg += "Computing right subtree size (id = " + rt.get(Role.RIGHT_CHILD) + ")...";
		        	this.print(msg, printMsg, data.getInitialNode());
	    			msg = new Message(destinationID, rt.get(Role.RIGHT_CHILD), new GetSubtreeSizeRequest(msgMode, data.getInitialNode()));
	    			send(msg);
					
	    		}
				return;
			}
			givenSize = leftSubtreeSize + rightSubtreeSize;
//			if (leftSubtreeSize != null && rightSubtreeSize != null){
//				data = new GetSubtreeSizeResponse(leftSubtreeSize + rightSubtreeSize, destinationID, data.getInitialNode());
//				storedMsgData.remove(LEFT_CHILD_SIZE);
//				storedMsgData.remove(RIGHT_CHILD_SIZE);
//				storedMsgData.remove(UNEVEN_CHILD);
//			}
//			else return;
//	    	msg.setData(data);
//			msg.setSourceId(id);
//			send(msg);
    	}
    	else
    		this.storedMsgData.put(Key.BUCKET_SIZE, givenSize);
    	//decide if a message needs to be sent
    	//if (mode == Mode.MODE_CHECK_BALANCE && (this.id == destinationID || this.isLeaf())){
		if (this.isRoot()) return;
    	if (this.id != destinationID && data.getMode() != Mode.REDISTRIBUTION){
    		if (rt.get(Role.PARENT) == destinationID){
    			String printMsg = "Node " + id + " is not in redistribution mode. Destination ID=" + destinationID + ". Performing a balance check on parent...";
        		this.print(msg, printMsg, data.getInitialNode());
    			msg = new Message(id, rt.get(Role.PARENT), new CheckBalanceRequest(givenSize, data.getInitialNode()));
    		}
    		else{
    			String printMsg = "Node " + id + " is not in redistribution mode. Destination ID=" + destinationID + ". Forwarding response to parent...";
        		this.print(msg, printMsg, data.getInitialNode());
    			msg = new Message(id, rt.get(Role.PARENT), new GetSubtreeSizeResponse(msgMode, givenSize, data.getDestinationID(), data.getInitialNode()));
    		}
    		send(msg);
    	}
    	//if ((this.id == destinationID || this.isLeaf()) && mode == Mode.MODE_CHECK_BALANCE ){
    	else if (this.id == destinationID && data.getMode() == Mode.CHECK_BALANCE ){
			String printMsg = "Node " + id + " is in check balance mode. Destination ID=" + destinationID + ". " + "Performing a balance check on ";
    		this.print(msg, printMsg, data.getInitialNode());
    		CheckBalanceRequest newData = new CheckBalanceRequest(givenSize, data.getInitialNode());
    		msg.setData(newData);
    		forwardCheckBalanceRequest(msg);
    	}
    	else if (this.isLeaf() && data.getMode() == Mode.REDISTRIBUTION){
    		long noofUncheckedBucketNodes = redistData.get(Key.UNCHECKED_BUCKET_NODES);
    		long noofUncheckedBuckets = redistData.get(Key.UNCHECKED_BUCKETS);
    		long subtreeID = redistData.get(Key.UNEVEN_SUBTREE_ID);
    		RedistributionRequest rData = new RedistributionRequest(noofUncheckedBucketNodes, noofUncheckedBuckets, subtreeID, data.getInitialNode());
    		msg = new Message(id, id, rData);
    		forwardBucketRedistributionRequest(msg);
    	}
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
    	assert msg.getData() instanceof CheckBalanceRequest;
    	CheckBalanceRequest data = (CheckBalanceRequest)msg.getData();
    	if (mode != Mode.NORMAL && mode != Mode.CHECK_BALANCE){
	    	String printMsg = "The bucket of node " + this.id + " is being redistributed. Let's wait a bit...";
	    	this.print(msg, printMsg, data.getInitialNode());
    		return;
        }
//    	int counter = 0;
//    	while (mode != Mode.NORMAL && mode != Mode.CHECK_BALANCE){
//    		counter++;
//    		if (counter > 5)
//    			return;
//	    	String printMsg = "The bucket of node " + this.id + " is being redistributed. Let's wait a bit...";
//	    	this.print(msg, printMsg, data.getInitialNode());
//	    	try {
//				Thread.sleep(1000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				printErr(e, data.getInitialNode());
//			}
//    	}
    	if (this.isBucketNode())
			printErr(new Exception(), data.getInitialNode());
		else if (this.isLeaf())
			storedMsgData.put(Key.BUCKET_SIZE, data.getTotalBucketSize());
    	else if (msg.getSourceId() == rt.get(Role.LEFT_CHILD))
    		this.storedMsgData.put(Key.LEFT_CHILD_SIZE, data.getTotalBucketSize());
    	else if (msg.getSourceId() == rt.get(Role.RIGHT_CHILD))
    		this.storedMsgData.put(Key.RIGHT_CHILD_SIZE, data.getTotalBucketSize());
    	else{
	    	String printMsg = "Node " + this.id +
	    			" is extending. This is the end of the line. Tough luck, sorry...";
	    	this.print(msg, printMsg, data.getInitialNode());
    		return;
    	}
    	Long leftSubtreeSize = this.storedMsgData.get(Key.LEFT_CHILD_SIZE);
    	Long rightSubtreeSize = this.storedMsgData.get(Key.RIGHT_CHILD_SIZE);
    	Long bucketSize = this.storedMsgData.get(Key.BUCKET_SIZE);
		//if (!this.isLeaf())
			//assert leftSubtreeSize != null || rightSubtreeSize != null;
    	if (this.isLeaf()){
			if (bucketSize == null){
				//we haven't accessed this leaf before, we need to compute the node's size
		    	String printMsg = "Node " + this.id +
		    			" is leaf and root. Computing bucket size...";
		    	this.print(msg, printMsg, data.getInitialNode());
				msg = new Message(id, id, new GetSubtreeSizeRequest(Mode.CHECK_BALANCE, data.getInitialNode()));
				if (mode == Mode.NORMAL) mode = Mode.CHECK_BALANCE;
				forwardGetSubtreeSizeRequest(msg);
			}
			else if (!isRoot()){
				String printMsg = "Node " + this.id +
		    			" is a leaf. Forwarding balance check request to parent...";
		    	this.print(msg, printMsg, data.getInitialNode());
				msg = new Message(id, rt.get(Role.PARENT), new CheckBalanceRequest(data.getTotalBucketSize(), data.getInitialNode()));
		    	send(msg);
			}
			else{
		    	String printMsg = "Node " + this.id +
		    			" is root and leaf. Performing extension test...";
		    	this.print(msg, printMsg, data.getInitialNode());
		    	//this.redistData.put(UNCHECKED_BUCKET_NODES, data.getTotalBucketSize());
		    	this.redistData.put(Key.UNCHECKED_BUCKET_NODES, bucketSize);
		    	this.redistData.put(Key.UNCHECKED_BUCKETS, 1L);
				msg = new Message(id, id, new ExtendContractRequest(bucketSize, 1, data.getInitialNode()));
				mode = Mode.NORMAL;
				this.forwardExtendContractRequest(msg);
			}
    	}
		else {
			if (leftSubtreeSize == null || rightSubtreeSize == null){
				assert leftSubtreeSize != null || rightSubtreeSize != null;
				//this isn't a leaf and some subtree data is missing
				Message msg1 = msg;
				if (leftSubtreeSize == null) //get left subtree size
					msg = new Message(id, rt.get(Role.LEFT_CHILD), new GetSubtreeSizeRequest(Mode.CHECK_BALANCE, data.getInitialNode()));
				else this.storedMsgData.put(Key.UNEVEN_CHILD, rt.get(Role.LEFT_CHILD));
				if (rightSubtreeSize == null) //get right subtree size
					msg = new Message(id, rt.get(Role.RIGHT_CHILD), new GetSubtreeSizeRequest(Mode.CHECK_BALANCE, data.getInitialNode()));
				else this.storedMsgData.put(Key.UNEVEN_CHILD, rt.get(Role.RIGHT_CHILD));
				String printMsg = msg.getDestinationId() == rt.get(Role.LEFT_CHILD) ? "left child..." : "right child..." ;
				printMsg = "This node is missing subtree data. Sending size request to its " + printMsg;
				this.print(msg1, printMsg, data.getInitialNode());
				send(msg);
	    	}
			else if (isBalanced()){
				
				long unevenChild = this.storedMsgData.get(Key.UNEVEN_CHILD);
				//if node is balanced
//				String printMsg = "";
//				if (this.isRoot()){
//					printMsg = "This is a balanced root ( |" + leftSubtreeSize + "| vs |" + rightSubtreeSize +
//		        			"| ). Doing an extension/contraction test...";
//		    		this.print(msg, printMsg, data.getInitialNode());
//		    		ExtendContractRequest ecData = new ExtendContractRequest(0, data.getInitialNode());
//		    		long target = rt.get(Role.LEFT_A_NODE) != RoutingTable.DEF_VAL ? rt.get(Role.LEFT_A_NODE) : rt.get(Role.RIGHT_A_NODE);
//		    		msg = new Message(id, target, ecData);
//		    		send(msg);
//				}
				if (rt.get(Role.LEFT_A_NODE) == rt.get(Role.LEFT_CHILD) || rt.get(Role.RIGHT_A_NODE) == rt.get(Role.RIGHT_CHILD)){
		        	String printMsg = "This is a balanced inner node ( |" + leftSubtreeSize + "| vs |" + rightSubtreeSize +
		        			"| ). Children " + rt.get(Role.LEFT_CHILD) + " and " + rt.get(Role.RIGHT_CHILD) +
		        			" are leaves and are always balanced. Nothing to redistribute here...";
		        	//printMsg += "Checking if the tree needs extension/contraction...";
		    		this.print(msg, printMsg, data.getInitialNode());
		    		
//		    		long totalSubtreeSize = leftSubtreeSize + rightSubtreeSize;
//		    		long totalBuckets = new Double(Math.pow(2, rt.getHeight())).longValue();
//		    		long totalBucketNodes = totalBuckets * totalSubtreeSize / 2;
		    		
//		    		ExtendContractRequest exData = new ExtendContractRequest(totalBucketNodes, rt.getHeight() + 1, data.getInitialNode());
//		    		msg = new Message(id, id, exData);
//		    		//send(msg);
//		    		forwardExtendContractRequest(msg);
		    	}
				else {
					String printMsg = "This is a balanced inner node ( |" + leftSubtreeSize + "| vs |" + rightSubtreeSize + "| ). Forwarding balance check request " +
						"to uneven child with id = " + unevenChild + "...";
					this.print(msg, printMsg, data.getInitialNode());
					Key key = rt.get(Role.LEFT_CHILD) == unevenChild ? Key.LEFT_CHILD_SIZE : Key.RIGHT_CHILD_SIZE;
					long totalSubtreeSize = this.storedMsgData.get(key);
					this.storedMsgData.remove(Key.LEFT_CHILD_SIZE);
					this.storedMsgData.remove(Key.RIGHT_CHILD_SIZE);
					msg = new Message(id, unevenChild, new RedistributionRequest(totalSubtreeSize, 1, unevenChild, data.getInitialNode()));
					send(msg);
					mode = Mode.REDISTRIBUTION;
				}
			}
			else{
				this.storedMsgData.remove(Key.LEFT_CHILD_SIZE);
				this.storedMsgData.remove(Key.RIGHT_CHILD_SIZE);
				mode = Mode.NORMAL;
				if (!isRoot()){
					String printMsg = "This is an unbalanced inner node ( |" + leftSubtreeSize + "| vs |" + rightSubtreeSize + "| ). Forwarding balance check request to parent...";
			    	this.print(msg, printMsg, data.getInitialNode());
					msg = new Message(id, rt.get(Role.PARENT), new CheckBalanceRequest(data.getTotalBucketSize(), data.getInitialNode()));
			    	send(msg);
				}
				else {
					String printMsg = "This is the root and it's unbalanced ( |" + leftSubtreeSize + "| vs |" + rightSubtreeSize + "| ). Performing full tree redistribution...";
			    	this.print(msg, printMsg, data.getInitialNode());
					long totalSubtreeSize = leftSubtreeSize + rightSubtreeSize;
					msg = new Message(id, id, new RedistributionRequest(totalSubtreeSize, 1, id, data.getInitialNode()));
					this.forwardBucketRedistributionRequest(msg);
				}
			}
		}
    }
    private boolean isBalanced(){
    	if (this.isLeaf())
    		return true;
    	Long leftSubtreeSize = this.storedMsgData.get(Key.LEFT_CHILD_SIZE);
    	Long rightSubtreeSize = this.storedMsgData.get(Key.RIGHT_CHILD_SIZE);
    	Long totalSize = leftSubtreeSize + rightSubtreeSize;
    	float nc = (float)leftSubtreeSize / (float)totalSize;
    	return nc > 0.25 && nc < 0.75;
    }
    void disconnect(Message msg) {
    	assert msg.getData() instanceof DisconnectMessage;
    	DisconnectMessage data = (DisconnectMessage)msg.getData();
    	long nodeToRemove = data.getNodeToRemove();
    	RoutingTable.Role role = data.getRole();
    	if (rt.get(role) == nodeToRemove)
    		rt.set(role, RoutingTable.DEF_VAL);
//    	switch (role){
//    	case BUCKET_NODE:
//    		if (rt.getBucketNode() == nodeToRemove) rt.setBucketNode(RoutingTable.DEF_VAL); break;
//		case REPRESENTATIVE:
//			if (rt.getRepresentative() == nodeToRemove) rt.setRepresentative(RoutingTable.DEF_VAL); break;
//		case LEFT_A_NODE:
//    		if (rt.getLeftAdjacentNode() == nodeToRemove) rt.setLeftAdjacentNode(RoutingTable.DEF_VAL); break;
//		case RIGHT_A_NODE:
//			if (rt.getRightAdjacentNode() == nodeToRemove) rt.setRightAdjacentNode(RoutingTable.DEF_VAL); break;
//		case LEFT_CHILD: 
//			if (rt.getLeftChild() == nodeToRemove) rt.setLeftChild(RoutingTable.DEF_VAL); break;
//		case RIGHT_CHILD:
//			if (rt.getRightChild() == nodeToRemove) rt.setRightChild(RoutingTable.DEF_VAL); break;
//		case PARENT:
//			if (rt.getParent() == nodeToRemove) rt.setParent(RoutingTable.DEF_VAL); break;
//		case LEFT_NEIGHBOR:
//			if (rt.getLeftRT().get(0) == nodeToRemove){
//				Vector<Long> leftRT = rt.getLeftRT();
//				leftRT.remove(0);
//				rt.setLeftRT(leftRT);
//			}
//			break;
//		case RIGHT_NEIGHBOR:
//			if (rt.getRightRT().get(0) == nodeToRemove){
//				Vector<Long> rightRT = rt.getRightRT();
//				rightRT.remove(0);
//				rt.setRightRT(rightRT);
//			}
//			break;
//		default:
//			break;
//    	}
    }
    void connect(Message msg) {
    	assert msg.getData() instanceof ConnectMessage;
    	ConnectMessage data = (ConnectMessage)msg.getData();
    	long nodeToAdd = data.getNode();
    	int index = data.getIndex();
    	RoutingTable.Role role = data.getRole();
//		String printMsg = "Setting " + nodeToAdd + " as the ";
    	rt.set(role, index, nodeToAdd);
//    	switch (role){
//    	case BUCKET_NODE: 
////    		printMsg += "bucket node of node " + id + "...";
////        	this.print(msg, printMsg, data.getInitialNode());
//    		if (data.replaces() || rt.getBucketNode() == RoutingTable.DEF_VAL)
//    			rt.setBucketNode(nodeToAdd); break;
//		case REPRESENTATIVE:
////    		printMsg += "representative of node " + id + "...";
////        	this.print(msg, printMsg, data.getInitialNode());
//    		if (data.replaces() || rt.getRepresentative() == RoutingTable.DEF_VAL)
//    			rt.setRepresentative(nodeToAdd); break;
//		case LEFT_A_NODE:
////    		printMsg += "left adjacent node of node " + id + "...";
////        	this.print(msg, printMsg, data.getInitialNode());
//    		if (data.replaces() || rt.getLeftAdjacentNode() == RoutingTable.DEF_VAL)
//    			rt.setLeftAdjacentNode(nodeToAdd); break;
//		case RIGHT_A_NODE:
////    		printMsg += "right adjacent node of node " + id + "...";
////        	this.print(msg, printMsg, data.getInitialNode());
//    		if (data.replaces() || rt.getRightAdjacentNode() == RoutingTable.DEF_VAL)
//    			rt.setRightAdjacentNode(nodeToAdd); break;
//		case LEFT_CHILD:
////    		printMsg += "left child of node " + id + "...";
////        	this.print(msg, printMsg, data.getInitialNode());
//    		if (data.replaces() || rt.getLeftChild() == RoutingTable.DEF_VAL)
//    			rt.setLeftChild(nodeToAdd); break;
//		case RIGHT_CHILD:
////    		printMsg += "right child of node " + id + "...";
////        	this.print(msg, printMsg, data.getInitialNode());
//    		if (data.replaces() || rt.getRightChild() == RoutingTable.DEF_VAL)
//    			rt.setRightChild(nodeToAdd); break;
//		case PARENT:
////    		printMsg += "parent of node " + id + "...";
////        	this.print(msg, printMsg, data.getInitialNode());
//    		if (data.replaces() || rt.getParent() == RoutingTable.DEF_VAL)
//    			rt.setParent(nodeToAdd); break;
//		case LEFT_NEIGHBOR:
////    		printMsg += "left neighbor of node " + id + "...";
////        	this.print(msg, printMsg, data.getInitialNode());
//			Vector<Long> leftRT = rt.getLeftRT();
//			if (data.replaces() && !leftRT.isEmpty())
//				leftRT.set(0, nodeToAdd);
//			else
//				leftRT.add(nodeToAdd);
//			rt.setLeftRT(leftRT);
//			break;
//		case RIGHT_NEIGHBOR:
////    		printMsg += "right neighbor of node " + id + "...";
////        	this.print(msg, printMsg, data.getInitialNode());
//    		Vector<Long> rightRT = rt.getRightRT();
//			if (data.replaces() && !rightRT.isEmpty())
//				rightRT.set(0, nodeToAdd);
//			else
//				rightRT.add(nodeToAdd);
//			rt.setRightRT(rightRT);
//			break;
//		case LEFT_RT:
//			leftRT = rt.getLeftRT();
//    		if (data.replaces() || leftRT.get(index) == RoutingTable.DEF_VAL){
//    			while (index >= leftRT.size()) leftRT.add(-1L);
//				leftRT.set(index, nodeToAdd);
//				rt.setLeftRT(leftRT);
//    		}
//			break;
//		case RIGHT_RT:
//			rightRT = rt.getRightRT();
//    		if (data.replaces() || rightRT.get(index) == RoutingTable.DEF_VAL){
//    			while (index >= rightRT.size()) rightRT.add(-1L);
//				rightRT.set(index, nodeToAdd);
//				rt.setRightRT(rightRT);
//    		}
//			break;
//		default:
//			break;
//    	}
    }
    void forwardLookupRequest(Message msg) {
    	assert msg.getData() instanceof LookupRequest;
        //throw new UnsupportedOperationException("Not supported yet.");
    	//System.out.println("Not supported yet.");
    }
    void printTree(Message msg) {
    	//TODO get to the root and then print downwards
    	PrintMessage data = (PrintMessage)msg.getData();
    	int srcMsgType = data.getSourceType();
    	if (id == msg.getSourceId()){
	    	for (int i = 0; i < data.getInitialNode(); i++){
	    		Message msg1 = new Message(id, i + 1, new PrintRTMessage(data.getInitialNode()));
	    		send(msg1);
	    	}
    	}
    	//if (srcMsgType == D2TreeMessageT.EXTEND_REQ || srcMsgType == D2TreeMessageT.TRANSFER_RES){
        if (srcMsgType != D2TreeMessageT.JOIN_REQ){
	    	String logFile = logDir + "main" + data.getInitialNode() + ".log";
	    	String allLogFile = logDir + "main.log";
	    	System.out.println("Saving log to " + logFile);
    		PrintWriter out = null;
			try {
				String msgType = isRoot() ? D2TreeMessageT.toString(data.getSourceType()) + "\n" : "";
					
				out = new PrintWriter(new FileWriter(logFile, true));
	    		out.format("%s MID=%5d, Id=%3d,", msgType, msg.getMsgId(), id);
	    		rt.print(out);
	    		out.close();

				out = new PrintWriter(new FileWriter(allLogFile, true));
	    		out.format("%s MID=%5d, Id=%3d,", msgType, msg.getMsgId(), id);
	    		rt.print(out);
	    		out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
	    	if (isRoot())
	    		data = new PrintMessage(true, srcMsgType, data.getInitialNode());
	    	if (data.goesDown()){
				msg.setData(data);
	    		if(isLeaf()){
	    			long destination = rt.get(Role.BUCKET_NODE);
	    			if (destination != RoutingTable.DEF_VAL)
	    				msg.setDestinationId(destination);
	    			send(msg);
	    		}
	    		else if (isBucketNode()){
	    			if (rt.getRightRT().isEmpty()) return;
	    			long destination = rt.getRightRT().get(0);
	    			if (destination != RoutingTable.DEF_VAL)
	    				msg.setDestinationId(destination);
	    			send(msg);
	    		}
	    		else{
	    			if (rt.get(Role.LEFT_CHILD) != RoutingTable.DEF_VAL && rt.get(Role.RIGHT_CHILD) != RoutingTable.DEF_VAL){
	    				msg = new Message(id, rt.get(Role.LEFT_CHILD), data);
	        			//msg.setDestinationId(rt.get(Role.LEFT_CHILD));
	        			send(msg);
	        			
	        			msg = new Message(id, rt.get(Role.RIGHT_CHILD), data);
	        			//msg.setDestinationId(rt.get(Role.RIGHT_CHILD));
	        			send(msg);
	    			}
	    			else
	    				send(msg);
	    		}
	    		if (msg.getDestinationId() == id) return;
	    	}
	    	else {
	    		if (this.isBucketNode())
	    			msg.setDestinationId(rt.get(Role.REPRESENTATIVE));
	    		else
	    			msg.setDestinationId(rt.get(Role.PARENT));
	    		send(msg);
	    	}
    	}
	}
    boolean isLeaf(){
    	//leaves don't have children or a representative
    	boolean itIs = !hasRepresentative() && !hasLeftChild() && !hasRightChild();
    	if (itIs && rt.get(Role.BUCKET_NODE) == RoutingTable.DEF_VAL){
    		try {
    			PrintWriter out = new PrintWriter(new FileWriter(logDir + "isLeaf.log", true));
    			new RuntimeException().printStackTrace(out);
    			out.print("ID = " + id + ", ");
	    		rt.print(out);
	    		out.close();
				Thread.sleep(2000);
				return !hasRepresentative() && !hasLeftChild() && !hasRightChild();
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	return !hasRepresentative() && !hasLeftChild() && !hasRightChild();
    }
    boolean isBucketNode(){
    	//bucketNodes have a representative
    	return hasRepresentative();
    }
    boolean isRoot(){
    	//the root has no parent and no representative 
    	return !hasParent() && !hasRepresentative();
    }
    boolean hasParent(){
    	return rt.get(Role.PARENT) != RoutingTable.DEF_VAL;
    }
    boolean hasRepresentative(){
    	return rt.get(Role.REPRESENTATIVE) != RoutingTable.DEF_VAL;
    }
    boolean hasLeftChild(){
    	return rt.get(Role.LEFT_CHILD) != RoutingTable.DEF_VAL;
    }
    boolean hasRightChild(){
    	return rt.get(Role.RIGHT_CHILD) != RoutingTable.DEF_VAL;
    }
    int getRtSize() {
        return rt.size();
    }
    
    void send(Message msg){
    	if (msg.getDestinationId() == RoutingTable.DEF_VAL){
    		NullPointerException ex = new NullPointerException();
    		ex.printStackTrace();
    		System.err.println(msg);
    		System.err.println(msg.getData());
    		//throw ex;
    	}
    	net.sendMsg(msg);
    }
    
    void printRT(Message msg){
    	PrintRTMessage data = (PrintRTMessage)msg.getData();
    	String logFile = logDir + "main" + data.getInitialNode() + ".txt";
    	String allLogFile = logDir + "main.log";
    	System.out.println("Saving log to " + logFile);
		PrintWriter out = null;
		try {
			out = new PrintWriter(new FileWriter(logFile, true));
    		out.format("MID=%3d, Id=%3d,", msg.getMsgId(), id);
    		rt.print(out);
    		out.close();

			out = new PrintWriter(new FileWriter(allLogFile, true));
    		out.format("MID=%3d, Id=%3d,", msg.getMsgId(), id);
    		rt.print(out);
    		out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
    /*void print(int msgType, String printMsg){
    	System.out.println(D2TreeMessageT.toString(msgType) +
    			": " + printMsg);
    }*/
    void print(Message msg, String printMsg, long initialNode){
    	try {
        	String logFile = logDir + "state" + initialNode + ".txt";
        	String allLogFile = logDir + "main.log";
        	System.out.println("Saving log to " + logFile);
            
        	PrintWriter out = new PrintWriter(new FileWriter(logFile, true));
            
            out.println("\n" + D2TreeMessageT.toString(msg.getType()) + "(MID = " + msg.getMsgId() +
	    			", NID = " + id + ", Initial node = " + initialNode + "): " + printMsg + " Hops = " + msg.getHops());
	    	out.close();
	    	
			out = new PrintWriter(new FileWriter(allLogFile, true));
			out.println("\n" + D2TreeMessageT.toString(msg.getType()) + "(MID = " + msg.getMsgId() +
	    			", NID = " + id + ", Initial node = " + initialNode + "): " + printMsg + " Hops = " + msg.getHops());
	    	out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    static void printErr(Exception ex, long initialNode){
		ex.printStackTrace();
    	try {
        	String logFile = logDir + "state" + initialNode + ".txt";
        	System.out.println("Saving log to " + logFile);
			PrintWriter out = new PrintWriter(new FileWriter(logFile, true));
			ex.printStackTrace(out);
	    	out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
