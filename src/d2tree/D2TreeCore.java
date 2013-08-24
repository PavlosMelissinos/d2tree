package d2tree;

import java.io.File;
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
	static final int LEFT_CHILD_SIZE        = 1001;
	static final int RIGHT_CHILD_SIZE       = 1002;
	static final int UNEVEN_CHILD           = 2000;
	static final int UNEVEN_SUBTREE_ID      = 3000;

	static final int BUCKET_SIZE            = 4000;
	static final int UNCHECKED_BUCKET_NODES = 5000;
	static final int UNCHECKED_BUCKETS      = 5001;
	static final int MODE                   = 6000;
	static final int DEST                   = 7000;

	/*static final long MODE_NORMAL         = 0L;
	static final long MODE_CHECK_BALANCE  = 1000L;
	static final long MODE_REDISTRIBUTION = 2000L;
	static final long MODE_TRANSFER       = 3000L;*/
	//static final int TRUE                 = 1;
	//static final int FALSE                = 0;
	
	//extend constants
	static final long CURRENT_BUCKET      = 0L;
	
    private RoutingTable rt;
    private Network net;
    private long id;
    HashMap<Integer, Long> storedMsgData;
    HashMap<Integer, Long> redistData;
    HashMap<Integer, Long> ecData;
    
    public enum Mode{
    	MODE_NORMAL,
    	MODE_CHECK_BALANCE,
    	MODE_REDISTRIBUTION,
    	MODE_TRANSFER;
    }
    
    private Mode mode;

    //TODO keep as singleton for simplicity, convert to Vector of keys later
    //private long key;

    D2TreeCore(long id, Network net) {
        this.rt       = new RoutingTable();
        this.net      = net;
        this.id       = id;
        storedMsgData = new HashMap<Integer, Long>();
        redistData    = new HashMap<Integer, Long>();
        ecData        = new HashMap<Integer, Long>();
        //storedMsgData.put(MODE, MODE_NORMAL);
        this.mode = Mode.MODE_NORMAL;
        File logDirFile = new File(logDir);
        if (!logDirFile.exists()) logDirFile.mkdir();
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
        
        //DEBUG
        
        if (isBucketNode()){
        	if (this.rt.getRightRT().isEmpty()) {//core is the last bucket node of the bucket
        		String printMsg = "Node " + newNodeId + " has been added to the bucket of " + rt.getRepresentative() +
                		". Forwarding balance check request to representative with id = " + rt.getRepresentative() + "...";
                ConnectMessage connData = new ConnectMessage(rt.getRepresentative(), Role.REPRESENTATIVE, newNodeId);
                net.sendMsg(new Message(id, newNodeId, connData));
                connData = new ConnectMessage(id, Role.LEFT_NEIGHBOR, newNodeId);
                net.sendMsg(new Message(id, newNodeId, connData));
            	this.print(msg, printMsg, newNodeId);

            	long rightNeighbor = newNodeId;
                Vector<Long> rightRT = new Vector<Long>();
                rightRT.add(rightNeighbor);
                this.rt.setRightRT(rightRT);
                long bucketSize = msg.getHops() + 1;
                msg = new Message(id, rt.getRepresentative(), new CheckBalanceRequest(bucketSize, newNodeId));
                net.sendMsg(msg);
                msg = new Message(id, id, new PrintMessage(false, newNodeId));
                printTree(msg);
        		
        	}
            else{ //forward to next bucket node
            	long rNeighborNode = rt.getRightRT().get(0);
        		String printMsg = "Node " + id + " is a bucket node. " +
        				"Forwarding request to right neighbor with id = " + rNeighborNode + "...";
            	this.print(msg, printMsg, newNodeId);
            	msg.setDestinationId(rNeighborNode);
                net.sendMsg(msg);
            }
        }
        else if (isLeaf()){
        	if (rt.getBucketNode() == RoutingTable.DEF_VAL){ //leaf doesn't have a bucket
        		String printMsg = "Node " + id + " is a bucketless leaf. " +
        				"Adding " + newNodeId + " as this node's bucket node...";
            	this.print(msg, printMsg, newNodeId);
                this.rt.setBucketNode(newNodeId);
                msg = new Message(id, newNodeId, new ConnectMessage(id, Role.REPRESENTATIVE, newNodeId));
                net.sendMsg(msg);
        	}
        	else{
        		String printMsg = "Node " + id + " is a leaf. " +
        				"Forwarding request to its bucket node (id = " + rt.getBucketNode() + ")...";
            	this.print(msg, printMsg, newNodeId);
        		msg = new Message(newNodeId, rt.getBucketNode(), new JoinRequest());
                net.sendMsg(msg);
        	}
        }
        else { //core is an inner node
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
    	assert msg.getData() instanceof RedistributionRequest;
    	RedistributionRequest data = (RedistributionRequest)msg.getData();
    	//if this is an inner node, then forward to right child
    	if (!this.isLeaf() && !this.isBucketNode()){
        	String printMsg = "Node " + id +" is an inner node. Forwarding request to " +
        			rt.getRightChild() + ".";
        	this.print(msg, printMsg, data.getInitialNode());
    		//this.redistData.clear();
    		long noofUncheckedBucketNodes = data.getNoofUncheckedBucketNodes();
    		long noofUncheckedBuckets = 2 * data.getNoofUncheckedBuckets();
    		long subtreeID = data.getSubtreeID();
    		msg.setDestinationId(rt.getRightChild());
    		msg.setData(new RedistributionRequest(noofUncheckedBucketNodes, noofUncheckedBuckets, subtreeID, data.getInitialNode()));
    		net.sendMsg(msg);
    		return;
    	}
    	else if (this.isBucketNode()) throw new UnsupportedOperationException("What are you doing here?");

    	
    	//We've reached a leaf, so now we need to figure out which buckets to tamper with
    	String printMsg = "Node " + id + " is a leaf of the uneven subtree." +
    					"Figuring out which buckets to tamper with.";
    	//this.print(msg.getType(), printMsg);
    	Long bucketSize = this.redistData.get(BUCKET_SIZE);
		//if it's the first time we visit the leaf, we need to prepare it for what is to come,
    	//that is compute the size of its bucket and set to "redistribution" mode
    	//if (storedMsgData.get(MODE) == MODE_NORMAL && bucketSize == null){
    	if (mode == Mode.MODE_NORMAL && bucketSize == null){
    		redistData.put(UNEVEN_SUBTREE_ID, data.getSubtreeID());
    		redistData.put(UNCHECKED_BUCKET_NODES, data.getNoofUncheckedBucketNodes());
    		redistData.put(UNCHECKED_BUCKETS, data.getNoofUncheckedBuckets());
    		//storedMsgData.put(MODE, MODE_REDISTRIBUTION);
    		mode = Mode.MODE_REDISTRIBUTION;
    		redistData.put(DEST, rt.getLeftRT().get(0));
    		//msg = new Message(id, rt.getBucketNode(), new GetSubtreeSizeRequest(data.getInitialNode()));
    		//net.sendMsg(msg);
    		//return;
    	}
    	else if (mode == Mode.MODE_CHECK_BALANCE) return;
    	
		//now that we know the size of the bucket, check 
		//if any nodes need to be transferred from/to this bucket
    	Long uncheckedBucketNodes = this.redistData.get(UNCHECKED_BUCKET_NODES);
    	Long uncheckedBuckets = this.redistData.get(UNCHECKED_BUCKETS);
    	long optimalBucketSize = uncheckedBucketNodes / uncheckedBuckets;
    	long spareNodes = uncheckedBucketNodes % uncheckedBuckets;
    	long diff = bucketSize - optimalBucketSize;
		Long dest = redistData.get(DEST);
		if (dest == id){
			//if (storedMsgData.get(MODE) == MODE_TRANSFER || diff == 0 || (diff == 1 && spareNodes > 0)){ //this bucket is dest and is ok, so forward to its left neighbor
			if (mode == Mode.MODE_TRANSFER || diff == 0 || (diff == 1 && spareNodes > 0)){ //this bucket is dest and is ok, so forward to its left neighbor
				//this.storedMsgData.put(MODE, MODE_REDISTRIBUTION);
				mode = Mode.MODE_REDISTRIBUTION;
				msg.setDestinationId(rt.getLeftRT().get(0));
			}
			else{ //this bucket is dest and not ok, so send a response to the source of this message
				msg = new Message(id, msg.getSourceId(), new RedistributionResponse(bucketSize, data.getInitialNode()));
				//this.storedMsgData.put(MODE, MODE_TRANSFER);
				mode = Mode.MODE_TRANSFER;
			}
    		net.sendMsg(msg);
		}
		else if (diff == 0 || (diff == 1 && spareNodes > 0)){//this bucket is ok, so move to the next one (if there is one)
    		this.redistData.clear();
    		//storedMsgData.put(MODE, MODE_NORMAL);
    		mode = Mode.MODE_NORMAL;
    		uncheckedBuckets--;
    		uncheckedBucketNodes -= bucketSize;
    		if (uncheckedBuckets == 0){
    			//TODO forward extend/contract request to the root of the subtree
    			long subtreeID = this.redistData.get(D2TreeCore.UNEVEN_SUBTREE_ID);
    			ExtendContractRequest ecData = new ExtendContractRequest(rt.getHeight(), data.getInitialNode());
    			msg = new Message(id, subtreeID, ecData);
    			net.sendMsg(msg);
    		}
    		else if (uncheckedBucketNodes == 0 || rt.getLeftRT().isEmpty()){
    			System.out.println(D2TreeMessageT.toString(msg.getType()) + ": Something went wrong: ");
    			String leftNeighbor = rt.getLeftRT().isEmpty() ? "None" : String.valueOf(rt.getLeftRT().get(0));
    			System.out.println(D2TreeMessageT.toString(msg.getType()) + ": Unchecked buckets: " + uncheckedBuckets + ", Unchecked Bucket Nodes: "
    			+ uncheckedBucketNodes + ", left neighbor: " + leftNeighbor);
    		}
    		long subtreeID = redistData.get(UNEVEN_SUBTREE_ID);
    		RedistributionRequest msgData = new RedistributionRequest(uncheckedBucketNodes, uncheckedBuckets, subtreeID, data.getInitialNode());
    		if (dest == rt.getLeftRT().get(0))
    			msgData.setTransferDest(RedistributionRequest.DEF_VAL);
    		msg = new Message(id, rt.getLeftRT().get(0), msgData);
    		net.sendMsg(msg);
    	}
    	else{ //nodes need to be transferred from/to this node)
    		//storedMsgData.put(MODE, MODE_TRANSFER);
    		mode = Mode.MODE_TRANSFER;
			if (dest == RedistributionRequest.DEF_VAL){ //this means we've just started the transfer process
				redistData.put(DEST, rt.getLeftRT().get(0));
    			dest = redistData.get(DEST);
			}
			data.setTransferDest(dest);
			msg = new Message(id, dest, data);
			net.sendMsg(msg);
    	}
    }
    void forwardBucketRedistributionResponse(Message msg){
    	assert msg.getData() instanceof RedistributionResponse;
    	long uncheckedBucketNodes = this.redistData.get(UNCHECKED_BUCKET_NODES);
    	long uncheckedBuckets = this.redistData.get(UNCHECKED_BUCKETS);
    	long subtreeID = this.redistData.get(UNEVEN_SUBTREE_ID);
    	long optimalBucketSize = uncheckedBucketNodes / uncheckedBuckets;
    	long bucketSize = this.redistData.get(BUCKET_SIZE);
    	long diff = bucketSize - optimalBucketSize;
    	RedistributionResponse data = (RedistributionResponse)msg.getData();
    	long destDiff = data.getDestSize() - optimalBucketSize;
    	if (diff * destDiff >= 0){ //both this bucket and dest have either more or less nodes
    		msg = new Message(id, msg.getSourceId(), new RedistributionRequest(uncheckedBucketNodes, uncheckedBuckets, subtreeID, data.getInitialNode()));
    		net.sendMsg(msg);
    	}
    	else{
    		if (diff > destDiff){ // |pivotBucket| > |destBucket|
    			//move nodes from pivotBucket to destBucket
    			msg = new Message(id, rt.getBucketNode(), new TransferRequest(msg.getSourceId(), id, true, data.getInitialNode()));
    		}
    		else{ // |pivotBucket| < |destBucket|
    			msg = new Message(id, msg.getSourceId(), new TransferRequest(msg.getSourceId(), id, true, data.getInitialNode()));
    		}
			net.sendMsg(msg);
    	}
    }
    void forwardTransferRequest(Message msg){
    	assert msg.getData() instanceof TransferRequest;
    	TransferRequest transfData = (TransferRequest)msg.getData();
    	boolean isFirstPass = transfData.isFirstPass(); //is this the first time we run this request
    	long destBucket = transfData.getDestBucket();
    	long pivotBucket = transfData.getPivotBucket();
    	if (this.isLeaf()){
    		msg.setDestinationId(rt.getBucketNode());
    		net.sendMsg(msg);
    		return;
    	}
    	//this is a bucket node
		if (!rt.getRightRT().isEmpty() && rt.getRepresentative() != pivotBucket){
			//we are at the dest bucket
			//forward request to right neighbor until we reach the last node in the bucket
    		msg.setDestinationId(rt.getRightRT().get(0));
    		net.sendMsg(msg);
    		return;
		}
		
		//this runs either on the last node of the dest bucket
		//or the first node of the pivot bucket
		Long bucketSize = redistData.get(D2TreeCore.BUCKET_SIZE);
		assert bucketSize != null;
		if (isFirstPass){
			if (rt.getRepresentative() != pivotBucket){
				//move this node from the dest bucket to pivot (as first pass)
				
				//remove the link from the left neighbor to this node
				msg = new Message(id, rt.getLeftRT().get(0), new DisconnectMessage(id, Role.RIGHT_NEIGHBOR, transfData.getInitialNode()));
				net.sendMsg(msg);
				
				//remove the link from this node to its left neighbor
				rt.setLeftRT(new Vector<Long>());
				
				//send message to the pivot bucket with the new node
				transfData = new TransferRequest(destBucket, pivotBucket, false, transfData.getInitialNode());
				msg = new Message(id, pivotBucket, transfData);
				net.sendMsg(msg);
			}
			else{
				//move this node from the pivot bucket to dest (as first pass)
				
				//remove the link from the right neighbor to this node
				msg = new Message(id, rt.getRightRT().get(0), new DisconnectMessage(id, Role.LEFT_NEIGHBOR, transfData.getInitialNode()));
				net.sendMsg(msg);
				
				//remove the link from this node to its left neighbor
				rt.setRightRT(new Vector<Long>());
				
				//send message to the dest bucket with the new node
				transfData = new TransferRequest(destBucket, pivotBucket, false, transfData.getInitialNode());
				msg = new Message(id, destBucket, transfData);
				net.sendMsg(msg);
			}
		}
		else{ //second pass
			if (rt.getRepresentative() != pivotBucket){
				//move pivotNode from the pivot bucket to dest (as second pass)
				
				long pivotNode = msg.getSourceId();
				
				//add a link from pivotNode to the representative of destNode
				ConnectMessage connData = new ConnectMessage(rt.getRepresentative(), Role.REPRESENTATIVE, transfData.getInitialNode());
				msg = new Message(id, pivotNode, connData);
				net.sendMsg(msg);
				
				//add a link from pivotNode to destNode
				connData = new ConnectMessage(id, Role.LEFT_NEIGHBOR, transfData.getInitialNode());
				msg = new Message(id, pivotNode, connData);
				net.sendMsg(msg);
				
				//add a link from destNode to pivotNode
				connData = new ConnectMessage(pivotNode, Role.RIGHT_NEIGHBOR, transfData.getInitialNode());
				msg = new Message(id, id, connData);
				connect(msg);

				TransferResponse respData = new TransferResponse(TransferType.NODE_ADDED, pivotBucket, transfData.getInitialNode());
				msg = new Message(id, destBucket, respData);
				net.sendMsg(msg);
				
				respData = new TransferResponse(TransferType.NODE_REMOVED, pivotBucket, transfData.getInitialNode());
				msg = new Message(id, pivotBucket, respData);
				net.sendMsg(msg);
			}
			else{
				//move destNode from the dest bucket to pivot (as second pass)
				
				long destNode = msg.getSourceId();
				
				//add a link from destNode to pivotNode's representative
				ConnectMessage connData = new ConnectMessage(rt.getRepresentative(), Role.REPRESENTATIVE, transfData.getInitialNode());
				msg = new Message(id, destNode, connData);
				net.sendMsg(msg);
				
				//add a link from pivotNode's representative to destNode
				connData = new ConnectMessage(destNode, Role.BUCKET_NODE, transfData.getInitialNode());
				msg = new Message(id, rt.getRepresentative(), connData);
				connect(msg);
				
				//add a link from pivotNode to destNode
				connData = new ConnectMessage(destNode, Role.LEFT_NEIGHBOR, transfData.getInitialNode());
				msg = new Message(id, id, connData);
				connect(msg);
				
				//add a link from destNode to pivotNode
				connData = new ConnectMessage(id, Role.RIGHT_NEIGHBOR, transfData.getInitialNode());
				msg = new Message(id, destNode, connData);
				net.sendMsg(msg);

				TransferResponse respData = new TransferResponse(TransferType.NODE_ADDED, pivotBucket, transfData.getInitialNode());
				msg = new Message(id, pivotBucket, respData);
				net.sendMsg(msg);
				
				respData = new TransferResponse(TransferType.NODE_REMOVED, pivotBucket, transfData.getInitialNode());
				msg = new Message(id, destBucket, respData);
				net.sendMsg(msg);
			}
		}
    }
    
    void forwardTransferResponse(Message msg){
    	assert msg.getData() instanceof TransferResponse;
    	TransferResponse data = (TransferResponse)msg.getData();
    	if (this.isLeaf()){
    		TransferType transfType = data.getTransferType();
    		long pivotBucket = data.getPivotBucket();
    		long bucketSize = redistData.get(D2TreeCore.BUCKET_SIZE);
    		bucketSize = transfType == TransferType.NODE_ADDED ? bucketSize + 1 : bucketSize - 1;
    		redistData.put(D2TreeCore.BUCKET_SIZE, bucketSize);
    		
    		boolean isDestBucket = redistData.get(D2TreeCore.DEST) == id;
    		if (isDestBucket){
    			RedistributionResponse rData = new RedistributionResponse(bucketSize, data.getInitialNode());
    			msg = new Message(id, pivotBucket, rData);
    			net.sendMsg(msg);
    		}
    	}
//    	long unevenSubtreeID = redistData.get(D2TreeCore.UNEVEN_SUBTREE_ID);
//    	if (unevenSubtreeID != id){
//    		msg.setDestinationId(unevenSubtreeID);
//    		net.sendMsg(msg);
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
    	if (!this.isRoot()) return; //We only extend and contract if the root was uneven.
    	
    	ExtendContractRequest data = (ExtendContractRequest)msg.getData();
    	
    	long treeHeight = data.getHeight();
    	//long pbtSize = (treeHeight + 1) * (treeHeight + 1) - 1;
    	long pbtSize = (long)Math.pow(2, treeHeight) - 1; 
    	long subtreeSize = pbtSize + redistData.get(D2TreeCore.UNCHECKED_BUCKET_NODES);
    	double optimalBucketSize  = Math.log(subtreeSize) / Math.log(2);
    	double averageBucketSize = (double)redistData.get(D2TreeCore.UNCHECKED_BUCKET_NODES) / (double)redistData.get(D2TreeCore.UNCHECKED_BUCKETS);
    	double factor = 2.0;
    	double offset = 4.0;
    	boolean shouldExtend   = factor * treeHeight + offset <= averageBucketSize;
    	boolean shouldContract = treeHeight >= averageBucketSize * factor + offset;
		String printMsg = "Tree height is " + treeHeight + " and average bucket size is " + averageBucketSize +
				" (bucket nodes are " + redistData.get(D2TreeCore.UNCHECKED_BUCKET_NODES) + " and there are " + 
				redistData.get(D2TreeCore.UNCHECKED_BUCKETS) + " unchecked buckets). ";
    	if (shouldExtend){
    		printMsg += "Initiating tree extension.";
    		print(msg, printMsg, data.getInitialNode());
    		ExtendRequest eData = new ExtendRequest(Math.round(optimalBucketSize), (long)averageBucketSize, data.getInitialNode());
    		msg = new Message(id, id, eData);
        	net.sendMsg(msg);
    	}
    	else if (shouldContract && !this.isLeaf()){
    		printMsg += "Initiating tree contraction.";
        	print(msg, printMsg, data.getInitialNode());
        	ContractRequest cData = new ContractRequest(Math.round(optimalBucketSize), data.getInitialNode());
    		msg = new Message(id, id, cData);
        	net.sendMsg(msg);
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

    	//go to the leftmost leaf of the tree, if not already there
    	if (!this.isBucketNode()){
    		if (this.isLeaf()){ //forward request to the bucket node
        		String printMsg = "Node " + id + " is a leaf. Forwarding request to bucket node with id = " + rt.getBucketNode() + "...";
            	//this.print(msg, printMsg, data.getInitialNode());
        		msg = new Message(msg.getSourceId(), rt.getBucketNode(), (ExtendRequest)msg.getData()); //reset hop count
    		}
    		else{ //forward request to the left child
        		String printMsg = "Node " + id + " is an inner node. Forwarding request to left child with id = " + rt.getLeftChild() + "...";
            	//this.print(msg, printMsg, data.getInitialNode());
    			msg.setDestinationId(rt.getLeftChild());
    		}
    		net.sendMsg(msg);
    		return;
    	}
    	
    	// this is a bucket node
    	long oldOptimalBucketSize = data.getOldOptimalBucketSize();
    	long optimalBucketSize = (oldOptimalBucketSize - 1) / 2; //trick, accounts for odd vs even optimal sizes
    	int counter = msg.getHops();
    	if (rt.getLeftRT().isEmpty()){//this is the first node of the bucket, make it a left leaf
    		String printMsg = "Node " + id + " is the first node of the bucket. Making it a left leaf...";
        	this.print(msg, printMsg, data.getInitialNode());
    		bucketNodeToLeftLeaf(data);
    	}
    	else if (counter == optimalBucketSize + 1 && !rt.getRightRT().isEmpty()){ //the left bucket is full, make this a right leaf
    		//forward extend response to the old leaf
    		long leftLeaf = msg.getSourceId();
    		long rightLeaf = id;
    		long oldLeaf = rt.getRepresentative();
    		
    		//the left bucket is full, forward a new extend request to the new (left) leaf

    		String printMsg = "Node " + id + " is the middle node of the bucket. Left leaf is the node with id = " +
    				leftLeaf + " and the old leaf has id = " + oldLeaf + ". Making " + id + " a right leaf...";
        	this.print(msg, printMsg, data.getInitialNode());

        	ExtendResponse exData = new ExtendResponse(0, leftLeaf, rightLeaf, data.getInitialNode());
    		msg = new Message(id, oldLeaf, exData);
    		net.sendMsg(msg);
    		
    		bucketNodeToRightLeaf(data);
    	}
    	else{
    		long newLeaf = msg.getSourceId();
    		rt.setRepresentative(newLeaf);
    		if (rt.getLeftRT().get(0) == newLeaf) //this is the first node of the new bucket
    			rt.setLeftRT(new Vector<Long>()); //disconnect from newLeaf
    		
    		if (!rt.getRightRT().isEmpty()){
        		String printMsg = "Node " + id + " is an unremarkable bucket node. Routing table has been built. Now forwarding request to its right neighbor with id = " + rt.getRightRT().get(0) + "...";
            	//this.print(msg, printMsg, data.getInitialNode());
	    		//forward to next bucket node
	    		msg.setDestinationId(rt.getRightRT().get(0));
	    		net.sendMsg(msg);
    		}
    		else{
        		String printMsg = "Node " + id + " is the last bucket node. Routing table has been built.";
            	this.print(msg, printMsg, data.getInitialNode());
            	msg = new Message(id, id, new PrintMessage(false, data.getInitialNode()));
            	printTree(msg);
    		}
    	}
    }

	void bucketNodeToLeftLeaf(ExtendRequest data){
		long oldLeaf = rt.getRepresentative();
		long rightNeighbor = rt.getRightRT().get(0);
		
		//forward the request to the right neighbor
		net.sendMsg(new Message(id, rightNeighbor, data));
		
		//make this the left child of the old leaf
		ConnectMessage connData = new ConnectMessage(this.id, Role.LEFT_CHILD, data.getInitialNode());
		net.sendMsg(new Message(id, oldLeaf, connData));

		//make this the left adjacent node of the old leaf
		connData = new ConnectMessage(this.id, Role.LEFT_A_NODE, data.getInitialNode());
		net.sendMsg(new Message(id, oldLeaf, connData));

		//disconnect the bucket node of the old leaf
		DisconnectMessage discData = new DisconnectMessage(this.id, Role.BUCKET_NODE, data.getInitialNode());
		net.sendMsg(new Message(id, oldLeaf, discData));
		
		//TODO newLeaf.leftAdjacentNode <== oldLeaf.leftAdjacentNode
		//TODO oldLeaf.leftAdjacentNode.rightAdjacentNode <== newLeaf.leftAdjacentNode
		
		//make the old leaf the parent of this node
		rt.setParent(oldLeaf);
		
		//make the old leaf the left adjacent node of this node
		rt.setRightAdjacentNode(oldLeaf);
		
		//disconnect the representative
		rt.setRepresentative(RoutingTable.DEF_VAL);
		
		//make the right neighbor the bucketNode of this node
		rt.setBucketNode(rightNeighbor);
		
		//empty routing tables
		rt.setRightRT(new Vector<Long>());
		rt.setLeftRT(new Vector<Long>());
		
		//TODO make new routing tables
		String printMsg = "Bucket node " + id + " successfully turned into a left leaf...";
    	this.print(new Message(id, id, data), printMsg, data.getInitialNode());
    }
    void bucketNodeToRightLeaf(ExtendRequest data){
		long oldLeaf = rt.getRepresentative();
		long rightNeighbor = rt.getRightRT().get(0);
		
		//forward the request to the right neighbor
		net.sendMsg(new Message(id, rightNeighbor, data));
		
		//make this the left child of the old leaf
		ConnectMessage connData = new ConnectMessage(this.id, Role.RIGHT_CHILD, data.getInitialNode());
		net.sendMsg(new Message(id, oldLeaf, connData));

		//make this the left adjacent node of the old leaf
		connData = new ConnectMessage(this.id, Role.RIGHT_A_NODE, data.getInitialNode());
		net.sendMsg(new Message(id, oldLeaf, connData));
		
		//disconnect from left neighbor as right neighbor
		DisconnectMessage discData = new DisconnectMessage(this.id, Role.RIGHT_NEIGHBOR, data.getInitialNode());
		net.sendMsg(new Message(id, rt.getLeftRT().get(0), discData));
		
		//TODO newLeaf.rightAdjacentNode <== oldLeaf.rightAdjacentNode
		//TODO oldLeaf.rightAdjacentNode.leftAdjacentNode <== newLeaf.rightAdjacentNode
		
		//make the old leaf the parent of this node
		rt.setParent(oldLeaf);
		
		//make the old leaf the left adjacent node of this node
		rt.setLeftAdjacentNode(oldLeaf);
		
		//disconnect the representative
		rt.setRepresentative(RoutingTable.DEF_VAL);
		
		//make the right neighbor the bucketNode of this node
		rt.setBucketNode(rightNeighbor);
		
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
    	long lChild = data.getLeftChild();
    	long rChild = data.getRightChild();
    	if (index == 0){
    		if (rt.getLeftAdjacentNode() != RoutingTable.DEF_VAL && rt.getLeftAdjacentNode() != lChild){ //add a link from left adjacent to left child
    			ConnectMessage connData = new ConnectMessage(lChild, Role.RIGHT_A_NODE, data.getInitialNode());
    			msg = new Message(id, rt.getLeftAdjacentNode(), connData);
    			net.sendMsg(msg);
    		}
			
    		if (lChild != RoutingTable.DEF_VAL && rt.getLeftAdjacentNode() != lChild){ //add a link from left child to left adjacent
    			ConnectMessage connData = new ConnectMessage(rt.getLeftAdjacentNode(), Role.LEFT_A_NODE, data.getInitialNode());
				msg = new Message(id, lChild, connData);
				net.sendMsg(msg);
    		}
	    	
    		if (rt.getRightAdjacentNode() != RoutingTable.DEF_VAL && rt.getRightAdjacentNode() != rChild){ //add a link from right adjacent to right child
    			ConnectMessage connData = new ConnectMessage(rChild, Role.LEFT_A_NODE, data.getInitialNode());
				msg = new Message(id, rt.getRightAdjacentNode(), connData);
				net.sendMsg(msg);
    		}
			
    		if (rChild != RoutingTable.DEF_VAL && rt.getRightAdjacentNode() != rChild){ //add a link from right child to right adjacent
    			ConnectMessage connData = new ConnectMessage(rt.getRightAdjacentNode(), Role.RIGHT_A_NODE, data.getInitialNode());
				msg = new Message(id, rChild, connData);
				net.sendMsg(msg);
    		}

			if (rChild != RoutingTable.DEF_VAL){ //add a link from right child to left child
    			ConnectMessage connData = new ConnectMessage(lChild, Role.LEFT_NEIGHBOR, data.getInitialNode());
    			msg = new Message(id, rChild, connData);
    			net.sendMsg(msg);
			}
			
			if (lChild != RoutingTable.DEF_VAL){ //add a link from left child to right child
				ConnectMessage connData = new ConnectMessage(rChild, Role.RIGHT_NEIGHBOR, data.getInitialNode());
    			msg = new Message(id, lChild, connData);
    			net.sendMsg(msg);
			}
			
			for (int i = 0; i < rt.getLeftRT().size(); i++){
				long node = rt.getLeftRT().get(i);
				data = new ExtendResponse(-i - 1, lChild, rChild, data.getInitialNode());
				msg.setData(data);
				msg.setDestinationId(node);
				net.sendMsg(msg);
			}
			for (int i = 0; i < rt.getRightRT().size(); i++){
				long node = rt.getRightRT().get(i);
				data = new ExtendResponse(i + 1, lChild, rChild, data.getInitialNode());
				msg.setData(data);
				msg.setDestinationId(node);
				net.sendMsg(msg);
			}
			//TODO forward extend requests to the new leaves if their size is not optimal
			//ExtendRequest exData = new ExtendRequest(ecData.get(D2TreeCore.), );
    	}
    	else{
    		if (index < 0 ){ //old leaf's left RT
        		if (index == -1){ //this is the left neighbor of the old leaf
        			
        			if (rt.getRightChild() != RoutingTable.DEF_VAL){ //add a link from this node's right child to the original left child
	        			ConnectMessage connData = new ConnectMessage(lChild, Role.RIGHT_NEIGHBOR, data.getInitialNode());
	        			msg = new Message(id, rt.getRightChild(), connData);
	        			net.sendMsg(msg);
        			}
        			
        			if (lChild != RoutingTable.DEF_VAL){ //add a link from the original left child to this node's right child
        				ConnectMessage connData = new ConnectMessage(rt.getRightChild(), Role.LEFT_NEIGHBOR, data.getInitialNode());
	        			msg = new Message(id, lChild, connData);
	        			net.sendMsg(msg);
        			}
        		}
        		if (rt.getLeftChild() != RoutingTable.DEF_VAL){ //add a link from this node's left child to the original left child
	    			ConnectMessage connData = new ConnectMessage(lChild, Role.RIGHT_RT, -index - 1, data.getInitialNode());
	    			msg = new Message(id, rt.getLeftChild(), connData);
	    			net.sendMsg(msg);
        		}
    			
        		if (lChild != RoutingTable.DEF_VAL){ //add a link from the original left child to this node's left child
        			ConnectMessage connData = new ConnectMessage(rt.getLeftChild(), Role.LEFT_RT, -index - 1, data.getInitialNode());
	    			msg = new Message(id, lChild, connData);
	    			net.sendMsg(msg);
        		}

        		if (rt.getRightChild() != RoutingTable.DEF_VAL){ //add a link from this node's right child to the original right child
        			ConnectMessage connData = new ConnectMessage(rChild, Role.RIGHT_RT, -index - 1, data.getInitialNode());
	    			msg = new Message(id, rt.getRightChild(), connData);
	    			net.sendMsg(msg);
        		}
    			
        		if (rChild != RoutingTable.DEF_VAL){ //add a link from the original right child to this node's right child
        			ConnectMessage connData = new ConnectMessage(rt.getRightChild(), Role.LEFT_RT, -index - 1, data.getInitialNode());
	    			msg = new Message(id, rChild, connData);
	    			net.sendMsg(msg);
        		}
    		}
    		else { //old leaf's right RT
    			if (index == 1){ //this is the right neighbor of the old leaf
        			
    				if (rt.getRightChild() != RoutingTable.DEF_VAL){ //add a link from this node's left child to the original right child
	        			ConnectMessage connData = new ConnectMessage(rChild, Role.LEFT_NEIGHBOR, data.getInitialNode());
	        			net.sendMsg(new Message(id, rt.getRightChild(), connData));
    				}
        			
        			if (rChild != RoutingTable.DEF_VAL){ //add a link from the original right child to this node's left child
        				ConnectMessage connData = new ConnectMessage(rt.getLeftChild(), Role.RIGHT_NEIGHBOR, data.getInitialNode());
        				net.sendMsg(new Message(id, rChild, connData));
        			}
        			
        		}
    			if (rt.getLeftChild() != RoutingTable.DEF_VAL){ //add a link from this node's left child to the original left child
	    			ConnectMessage connData = new ConnectMessage(lChild, Role.LEFT_RT, index - 1, data.getInitialNode());
	    			msg = new Message(id, rt.getLeftChild(), connData);
	    			net.sendMsg(msg);
    			}
    			
    			if (lChild != RoutingTable.DEF_VAL){ //add a link from the original left child to this node's left child
	    			ConnectMessage connData = new ConnectMessage(rt.getLeftChild(), Role.RIGHT_RT, index - 1, data.getInitialNode());
	    			msg = new Message(id, lChild, connData);
	    			net.sendMsg(msg);
    			}

    			if (rt.getRightChild() != RoutingTable.DEF_VAL){ //add a link from this node's right child to the original right child
	    			ConnectMessage connData = new ConnectMessage(rChild, Role.RIGHT_RT, index - 1, data.getInitialNode());
	    			msg = new Message(id, rt.getRightChild(), connData);
	    			net.sendMsg(msg);
    			}
    			
    			if (rChild != RoutingTable.DEF_VAL){ //add a link from the original right child to this node's right child
	    			ConnectMessage connData = new ConnectMessage(rt.getRightChild(), Role.LEFT_RT, index - 1, data.getInitialNode());
	    			msg = new Message(id, rChild, connData);
	    			net.sendMsg(msg);
    			}
    		}
    	}
    }
    void forwardContractRequest(Message msg){
    	assert msg.getData() instanceof ContractRequest;
		//TODO contract
    }
    void forwardGetSubtreeSizeRequest(Message msg){
    	assert msg.getData() instanceof GetSubtreeSizeRequest;
    	if (this.isBucketNode()){
    		Vector<Long> rightRT = rt.getRightRT();
			GetSubtreeSizeRequest msgData = (GetSubtreeSizeRequest)msg.getData();
			//TODO Fix bug (forwardGetSubtreeSizeRequest is seemingly called twice for nodes that are last in their buckets)
			msgData.incrementSize();
			msg.setData(msgData);
    		if (rightRT.isEmpty()){ //node is last in its bucket
    			//long bucketSize = msg.getHops() - 1;
    			long bucketSize = msgData.getSize();
        		String printMsg = "This node is the last in its bucket (size = " + bucketSize +
    					"). Sending response to its representative, with id " + rt.getRepresentative() + ".";
		    	this.print(msg, printMsg, msgData.getInitialNode());
    			msg = new Message(id, rt.getRepresentative(),
    					new GetSubtreeSizeResponse(bucketSize, msg.getSourceId(), msgData.getInitialNode()));
    			net.sendMsg(msg);
    		}
    		else {
		    	String printMsg = "Node " + this.id +
		    			". Forwarding request to its right neighbor, with id " + rt.getRightRT().get(0) +
		    			". (size = " + msgData.getSize() + ")";
		    	//this.print(msg, printMsg);
    			long rightNeighbour = rightRT.get(0);
    			msg.setDestinationId(rightNeighbour);
    			net.sendMsg(msg);
    		}
    	}
    	else if (this.isLeaf()){
	    	String printMsg = "This is a leaf. Forwarding to bucket node with id "
	    			+ rt.getBucketNode() + ".";
	    	//this.print(msg, printMsg);
    		msg.setDestinationId(rt.getBucketNode());
    		net.sendMsg(msg);
    	}
    	else{
	    	String printMsg = "This is an inner node. Forwarding to its children with ids "
	    			+ rt.getLeftChild() + " and " + rt.getRightChild() + ".";
	    	//this.print(msg, printMsg);
    		msg.setDestinationId(rt.getLeftChild());
    		net.sendMsg(msg);
    		msg.setDestinationId(rt.getRightChild());
    		net.sendMsg(msg);
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
    	long givenSize = data.getSize();
    	long destinationID = data.getDestinationID();

//    	if (mode == Mode.MODE_CHECK_BALANCE && this.isLeaf()){
//        	this.storedMsgData.put(D2TreeCore.BUCKET_SIZE, givenSize);
//    		printMsg = "This is a leaf. " + printMsg;
//    		this.print(msg, printMsg, data.getInitialNode());
//    	}
    	//only deals with leaves
    	/*else if (mode == Mode.MODE_REDISTRIBUTION && this.isLeaf() && id == destinationID){
    		//TODO check if this.isLeaf() has to be asserted instead of being a condition
			printMsg = "Node " + id + " is in redistribution mode.";
	    	this.print(msg, printMsg);
			//we've just found out how many nodes this bucket contains so 
			//we now know if we need to add or remove any nodes from here
			
			this.redistData.put(BUCKET_SIZE, givenSize);
			long uncheckedBucketNodes = this.redistData.get(UNCHECKED_BUCKET_NODES);
			long subtreeID = this.redistData.get(UNEVEN_SUBTREE_ID);
			RedistributionRequest redistData = new RedistributionRequest(uncheckedBucketNodes, givenSize, subtreeID);
			
			mode = Mode.MODE_TRANSFER;
			//net.sendMsg(msg);
			forwardBucketRedistributionRequest(new Message(id, id, redistData));
			return;
		}*/
		//determine what the total size of the node's subtree is
    	//if this is leaf, data.getSize() gives us the size of its bucket, which coincidentally is also the size of its subtree
    	if (!this.isLeaf()){
    		String printMsg = "This is not a leaf. Destination ID = " + destinationID + ". (bucket size = " + data.getSize() + ") Mode = " + mode;
        	this.print(msg, printMsg, data.getInitialNode());
			int key = rt.getLeftChild() == msg.getSourceId() ? LEFT_CHILD_SIZE : RIGHT_CHILD_SIZE;
			this.storedMsgData.put(key, givenSize);
			Long leftSubtreeSize = this.storedMsgData.get(LEFT_CHILD_SIZE);
			Long rightSubtreeSize = this.storedMsgData.get(RIGHT_CHILD_SIZE);
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
//			net.sendMsg(msg);
    	}
    	//decide if a message needs to be sent
    	//if (mode == Mode.MODE_CHECK_BALANCE && (this.id == destinationID || this.isLeaf())){
		if (this.isRoot()) return;
    	if (this.id != destinationID && mode != Mode.MODE_REDISTRIBUTION){
    		if (rt.getParent() == destinationID){
    			String printMsg = "Node " + id + " is not in redistribution mode. Destination ID=" + destinationID + ". Performing a balance check on parent...";
        		this.print(msg, printMsg, data.getInitialNode());
    			msg = new Message(id, rt.getParent(), new CheckBalanceRequest(givenSize, data.getInitialNode()));
    		}
    		else{
    			String printMsg = "Node " + id + " is not in redistribution mode. Destination ID=" + destinationID + ". Forwarding response to parent...";
        		this.print(msg, printMsg, data.getInitialNode());
	    		msg.setSourceId(id);
				msg.setDestinationId(rt.getParent());
    		}
    		net.sendMsg(msg);
    	}
    	//if ((this.id == destinationID || this.isLeaf()) && mode == Mode.MODE_CHECK_BALANCE ){
    	else if (this.id == destinationID && mode == Mode.MODE_CHECK_BALANCE ){
			String printMsg = "Node " + id + " is in check balance mode. Destination ID=" + destinationID + ". " + "Performing a balance check on ";
    		this.print(msg, printMsg, data.getInitialNode());
    		CheckBalanceRequest newData = new CheckBalanceRequest(givenSize, data.getInitialNode());
    		msg.setData(newData);
    		forwardCheckBalanceRequest(msg);
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
    	assert mode == Mode.MODE_NORMAL || mode == Mode.MODE_CHECK_BALANCE;
    	if (this.isBucketNode())
    		new Exception().printStackTrace();
		else if (this.isLeaf())
			redistData.put(BUCKET_SIZE, data.getTotalBucketSize());
    	else if (msg.getSourceId() == rt.getLeftChild())
    		this.storedMsgData.put(LEFT_CHILD_SIZE, data.getTotalBucketSize());
    	else if (msg.getSourceId() == rt.getRightChild())
    		this.storedMsgData.put(RIGHT_CHILD_SIZE, data.getTotalBucketSize());
    	else{
	    	String printMsg = "Node " + this.id +
	    			" is extending. This is the end of the line. Tough luck, sorry...";
	    	this.print(msg, printMsg, data.getInitialNode());
    		return;
    	}
    	Long leftSubtreeSize = this.storedMsgData.get(LEFT_CHILD_SIZE);
    	Long rightSubtreeSize = this.storedMsgData.get(RIGHT_CHILD_SIZE);
    	Long bucketSize = this.redistData.get(BUCKET_SIZE);
		//if (!this.isLeaf())
			//assert leftSubtreeSize != null || rightSubtreeSize != null;
    	if (this.isLeaf()){
			if (bucketSize == null){
				//we haven't accessed this leaf before, we need to compute the node's size
		    	String printMsg = "Node " + this.id +
		    			" is leaf and root. Computing bucket size...";
		    	this.print(msg, printMsg, data.getInitialNode());
				msg = new Message(id, id, new GetSubtreeSizeRequest(data.getInitialNode()));
				if (mode == Mode.MODE_NORMAL) mode = Mode.MODE_CHECK_BALANCE;
				forwardGetSubtreeSizeRequest(msg);
			}
			else if (!isRoot()){
				String printMsg = "Node " + this.id +
		    			" is a leaf. Forwarding balance check request to parent...";
		    	this.print(msg, printMsg, data.getInitialNode());
				msg = new Message(id, rt.getParent(), new CheckBalanceRequest(data.getTotalBucketSize(), data.getInitialNode()));
		    	net.sendMsg(msg);
			}
			else{
		    	String printMsg = "Node " + this.id +
		    			" is root and leaf. Performing extension test...";
		    	this.print(msg, printMsg, data.getInitialNode());
		    	//this.redistData.put(UNCHECKED_BUCKET_NODES, data.getTotalBucketSize());
		    	this.redistData.put(UNCHECKED_BUCKET_NODES, bucketSize);
		    	this.redistData.put(UNCHECKED_BUCKETS, 1L);
				msg = new Message(id, id, new ExtendContractRequest(1, data.getInitialNode()));
				mode = Mode.MODE_NORMAL;
				this.forwardExtendContractRequest(msg);
			}
    	}
		else {
			if (leftSubtreeSize == null || rightSubtreeSize == null){
				assert leftSubtreeSize != null || rightSubtreeSize != null;
				//this isn't a leaf and some subtree data is missing
				Message msg1 = msg;
				if (leftSubtreeSize == null) //get left subtree size
					msg = new Message(id, rt.getLeftChild(), new GetSubtreeSizeRequest(data.getInitialNode()));
				else this.storedMsgData.put(UNEVEN_CHILD, rt.getLeftChild());
				if (rightSubtreeSize == null) //get right subtree size
					msg = new Message(id, rt.getRightChild(), new GetSubtreeSizeRequest(data.getInitialNode()));
				else this.storedMsgData.put(UNEVEN_CHILD, rt.getRightChild());
				String printMsg = msg.getDestinationId() == rt.getLeftChild() ? "left child..." : "right child..." ;
				printMsg = "This node is missing subtree data. Sending size request to its " + printMsg;
				this.print(msg1, printMsg, data.getInitialNode());
				net.sendMsg(msg);
	    	}
			else if (isBalanced()){
				
				long unevenChild = this.storedMsgData.get(UNEVEN_CHILD);
				//if node is
				String printMsg = "";
				if (rt.getLeftAdjacentNode() == rt.getLeftChild() || rt.getRightAdjacentNode() == rt.getRightChild()){
		        	printMsg = "This is a balanced inner node ( |" + leftSubtreeSize + "| vs |" + rightSubtreeSize +
		        			"| ). Children " + rt.getLeftChild() + " and " + rt.getRightChild() +
		        			" are leaves and are always balanced. Nothing to redistribute here. End of the line...";
		    		this.print(msg, printMsg, data.getInitialNode());
		        	return;
		    	}
				else printMsg = "This is a balanced inner node ( |" + leftSubtreeSize + "| vs |" + rightSubtreeSize + "| ). Forwarding balance check request " +
						"to uneven child with id = " + unevenChild + "...";
	    		this.print(msg, printMsg, data.getInitialNode());
				int key = rt.getLeftChild() == UNEVEN_CHILD ? LEFT_CHILD_SIZE : RIGHT_CHILD_SIZE;
				long totalSubtreeSize = this.storedMsgData.get(key);
				msg = new Message(id, unevenChild, new RedistributionRequest(totalSubtreeSize, 1, unevenChild, data.getInitialNode()));
				net.sendMsg(msg);
				mode = Mode.MODE_REDISTRIBUTION;
			}
			else{
				mode = Mode.MODE_NORMAL;
				if (!isRoot()){
					String printMsg = "This is an unbalanced inner node ( |" + leftSubtreeSize + "| vs |" + rightSubtreeSize + "| ). Forwarding balance check request to parent...";
			    	this.print(msg, printMsg, data.getInitialNode());
					msg = new Message(id, rt.getParent(), new CheckBalanceRequest(data.getTotalBucketSize(), data.getInitialNode()));
			    	net.sendMsg(msg);
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
    	Long leftSubtreeSize = this.storedMsgData.get(LEFT_CHILD_SIZE);
    	Long rightSubtreeSize = this.storedMsgData.get(RIGHT_CHILD_SIZE);
    	Long totalSize = leftSubtreeSize + rightSubtreeSize;
    	float nc = (float)leftSubtreeSize / (float)totalSize;
    	return nc > 0.25 && nc < 0.75;
    }
    void disconnect(Message msg) {
    	assert msg.getData() instanceof DisconnectMessage;
    	DisconnectMessage data = (DisconnectMessage)msg.getData();
    	long nodeToRemove = data.getNodeToRemove();
    	RoutingTable.Role role = data.getRole();
    	switch (role){
    	case BUCKET_NODE:
    		if (rt.getBucketNode() == nodeToRemove) rt.setBucketNode(RoutingTable.DEF_VAL); break;
		case REPRESENTATIVE:
			if (rt.getRepresentative() == nodeToRemove) rt.setRepresentative(RoutingTable.DEF_VAL); break;
		case LEFT_A_NODE:
    		if (rt.getLeftAdjacentNode() == nodeToRemove) rt.setLeftAdjacentNode(RoutingTable.DEF_VAL); break;
		case RIGHT_A_NODE:
			if (rt.getRightAdjacentNode() == nodeToRemove) rt.setRightAdjacentNode(RoutingTable.DEF_VAL); break;
		case LEFT_CHILD: 
			if (rt.getLeftChild() == nodeToRemove) rt.setLeftChild(RoutingTable.DEF_VAL); break;
		case RIGHT_CHILD:
			if (rt.getRightChild() == nodeToRemove) rt.setRightChild(RoutingTable.DEF_VAL); break;
		case PARENT:
			if (rt.getParent() == nodeToRemove) rt.setParent(RoutingTable.DEF_VAL); break;
		case LEFT_NEIGHBOR:
			if (rt.getLeftRT().get(0) == nodeToRemove){
				Vector<Long> leftRT = rt.getLeftRT();
				leftRT.remove(0);
				rt.setLeftRT(leftRT);
			}
			break;
		case RIGHT_NEIGHBOR:
			if (rt.getRightRT().get(0) == nodeToRemove){
				Vector<Long> rightRT = rt.getRightRT();
				rightRT.remove(0);
				rt.setRightRT(rightRT);
			}
			break;
		default:
			break;
    	}
    }
    void connect(Message msg) {
    	assert msg.getData() instanceof ConnectMessage;
    	ConnectMessage data = (ConnectMessage)msg.getData();
    	long nodeToAdd = data.getNode();
    	RoutingTable.Role role = data.getRole();
    	switch (role){
    	case BUCKET_NODE:
    		rt.setBucketNode(nodeToAdd); break;
		case REPRESENTATIVE:
			rt.setRepresentative(nodeToAdd); break;
		case LEFT_A_NODE:
    		rt.setLeftAdjacentNode(nodeToAdd); break;
		case RIGHT_A_NODE:
			rt.setRightAdjacentNode(nodeToAdd); break;
		case LEFT_CHILD: 
			rt.setLeftChild(nodeToAdd); break;
		case RIGHT_CHILD:
			rt.setRightChild(nodeToAdd); break;
		case PARENT:
			rt.setParent(nodeToAdd); break;
		case LEFT_NEIGHBOR:
			Vector<Long> leftRT = rt.getLeftRT();
			if (!leftRT.isEmpty())
				leftRT.set(0, nodeToAdd);
			else
				leftRT.add(nodeToAdd);
			rt.setLeftRT(leftRT);
			break;
		case RIGHT_NEIGHBOR:
			Vector<Long> rightRT = rt.getRightRT();
			if (!rightRT.isEmpty())
				rightRT.set(0, nodeToAdd);
			else
				rightRT.add(nodeToAdd);
			rt.setRightRT(rightRT);
			break;
		default:
			break;
    	}
    }
    void forwardLookupRequest(Message msg) {
    	assert msg.getData() instanceof LookupRequest;
        //throw new UnsupportedOperationException("Not supported yet.");
    	//System.out.println("Not supported yet.");
    }
    void printTree(Message msg) {
    	//TODO get to the root and then print downwards
    	PrintMessage data = (PrintMessage)msg.getData();
    	String logFile = logDir + "main" + data.getInitialNode() + ".log";
    	System.out.println("Saving log to " + logFile);
    	if (isRoot())
    		data = new PrintMessage(true, data.getInitialNode());
    	if (data.goesDown()){
    		PrintWriter out = null;
			try {
				out = new PrintWriter(new FileWriter(logFile, true));
	    		out.print("Id = " + id + ", ");
	    		rt.print(out);
	    		out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			msg.setData(data);
    		if(isLeaf()){
    			msg.setDestinationId(rt.getBucketNode());
    			net.sendMsg(msg);
    		}
    		else if (isBucketNode()){
    			if (rt.getRightRT().isEmpty()) return;
    			msg.setDestinationId(rt.getRightRT().get(0));
    			net.sendMsg(msg);
    		}
    		else{
    			msg = new Message(id, rt.getLeftChild(), data);
    			//msg.setDestinationId(rt.getLeftChild());
    			net.sendMsg(msg);
    			msg = new Message(id, rt.getRightChild(), data);
    			//msg.setDestinationId(rt.getRightChild());
    			net.sendMsg(msg);
    		}
    	}
    	else {
    		if (this.isBucketNode())
    			msg.setDestinationId(rt.getRepresentative());
    		else
    			msg.setDestinationId(rt.getParent());
    		net.sendMsg(msg);
    	}
	}
    boolean isLeaf(){
    	//leaves don't have children or a representative
    	boolean itIs = !hasRepresentative() && !hasLeftChild() && !hasRightChild();
    	if (itIs && rt.getBucketNode() == RoutingTable.DEF_VAL){
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
    	return rt.getParent() != RoutingTable.DEF_VAL;
    }
    boolean hasRepresentative(){
    	return rt.getRepresentative() != RoutingTable.DEF_VAL;
    }
    boolean hasLeftChild(){
    	return rt.getLeftChild() != RoutingTable.DEF_VAL;
    }
    boolean hasRightChild(){
    	return rt.getRightChild() != RoutingTable.DEF_VAL;
    }
    int getRtSize() {
        return rt.size();
    }
    long getNodeLevel(){
    	return Math.max(rt.getLeftRT().size(), rt.getRightRT().size());
    }
    
    /*void print(int msgType, String printMsg){
    	System.out.println(D2TreeMessageT.toString(msgType) +
    			": " + printMsg);
    }*/
    void print(Message msg, String printMsg, long initialNode){
    	try {
        	String logFile = logDir + "state" + initialNode + ".log";
			PrintWriter out = new PrintWriter(new FileWriter(logFile, true));
	    	out.println("\n" + D2TreeMessageT.toString(msg.getType()) + "(MID = " + msg.getMsgId() +
	    			", NID = " + id + ", Initial node = " + initialNode + "): " + printMsg + " Hops = " + msg.getHops());
	    	out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
