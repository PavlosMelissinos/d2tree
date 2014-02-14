package d2tree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import d2tree.RoutingTable.Role;

public class DataExtractor {
    static HashMap<Long, ArrayList<Long>> getBucketNodes(
            HashMap<Long, RoutingTable> peers) {
        HashMap<Long, ArrayList<Long>> bucketNodes = new HashMap<Long, ArrayList<Long>>();
        for (Long peerId : peers.keySet()) {
            RoutingTable peerRT = peers.get(peerId);
            if (peerRT.isBucketNode()) {
                long leafId = peerRT.get(Role.REPRESENTATIVE);
                if (!bucketNodes.containsKey(leafId))
                    bucketNodes.put(leafId, new ArrayList<Long>());
                ArrayList<Long> buckets = bucketNodes.get(leafId);
                buckets.add(peerId);
                bucketNodes.put(leafId, buckets);
            }
        }
        return bucketNodes;
    }

    static ArrayList<Long> getOrderedBucketNodes(
            HashMap<Long, RoutingTable> myRoutingTables, long bucketId) {
        ArrayList<Long> bucketNodes = new ArrayList<Long>();
        RoutingTable leafRT = myRoutingTables.get(bucketId);
        Long bucketNodeId = leafRT.get(Role.FIRST_BUCKET_NODE);
        bucketNodes.add(bucketNodeId);
        RoutingTable bucketNodeRT = myRoutingTables.get(bucketNodeId);
        while (!bucketNodeRT.isEmpty(Role.RIGHT_RT)) {
            bucketNodeId = bucketNodeRT.get(Role.RIGHT_RT, 0);
            if (bucketNodes.contains(bucketNodeId))
                bucketNodes.add(bucketNodeId);
            bucketNodes.add(bucketNodeId);
            bucketNodeRT = myRoutingTables.get(bucketNodeId);
        }

        return bucketNodes;
    }

    static ArrayList<Long> getBucketNodes(List<D2TreeCore> peers,
            D2TreeCore leaf) {
        if (!leaf.isLeaf()) return null;
        ArrayList<Long> bucketNodes = new ArrayList<Long>();
        for (D2TreeCore peer : peers) {
            long representative = peer.getRT().get(Role.REPRESENTATIVE);
            if (representative == leaf.id) bucketNodes.add(peer.id);
        }
        return bucketNodes;
    }

    static ArrayList<Long> getBucketNodes(long bucket) {
        ArrayList<Long> bucketNodes = new ArrayList<Long>();
        RoutingTable rt = D2TreeCore.routingTables.get(bucket);
        long bucketNode = rt.get(Role.FIRST_BUCKET_NODE);
        while (bucketNode != RoutingTable.DEF_VAL &&
                !bucketNodes.contains(bucketNode)) {
            bucketNodes.add(bucketNode);
            rt = D2TreeCore.routingTables.get(bucketNode);
            bucketNode = rt.get(Role.RIGHT_RT, 0);
        }
        return bucketNodes;
    }
    // TODO Finish the method
    // static boolean contains(long bucket, long bucketNode) {
    // ArrayList<Long> bucketNodes = new ArrayList<Long>();
    // RoutingTable rt = D2TreeCore.routingTables.get(bucket);
    // long bucketNode = rt.get(Role.FIRST_BUCKET_NODE);
    // while (bucketNode != RoutingTable.DEF_VAL &&
    // !bucketNodes.contains(bucketNode)) {
    // bucketNodes.add(bucketNode);
    // rt = D2TreeCore.routingTables.get(bucketNode);
    // bucketNode = rt.get(Role.RIGHT_RT, 0);
    // }
    // return bucketNodes;
    //
    // }

    // static ArrayList<Long> getChildren(HashMap<Long, RoutingTable> peers,
    // long node) {
    //
    // }
}
