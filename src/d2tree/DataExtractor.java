package d2tree;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeSet;

import d2tree.RoutingTable.Role;

public class DataExtractor {
    static LinkedHashMap<Long, ArrayList<Long>> getBucketNodes(
            LinkedHashMap<Long, RoutingTable> peers) {
        LinkedHashMap<Long, ArrayList<Long>> bucketNodes = new LinkedHashMap<Long, ArrayList<Long>>();
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

    static LinkedHashMap<Long, TreeSet<Long>> getOrderedBucketNodes(
            LinkedHashMap<Long, RoutingTable> myRoutingTables, long bucketId) {
        LinkedHashMap<Long, TreeSet<Long>> bucketNodes = new LinkedHashMap<Long, TreeSet<Long>>();
        RoutingTable leafRT = myRoutingTables.get(bucketId);
        Long bucketNodeId = leafRT.get(Role.FIRST_BUCKET_NODE);
        LinkedHashMap<Long, TreeSet<Long>> keys = D2Tree.getAllKeys();

        assert bucketNodeId != RoutingTable.DEF_VAL;

        bucketNodes.put(bucketNodeId, keys.get(bucketNodeId));
        RoutingTable bucketNodeRT = myRoutingTables.get(bucketNodeId);
        while (!bucketNodeRT.isEmpty(Role.RIGHT_RT)) {
            bucketNodeId = bucketNodeRT.get(Role.RIGHT_RT, 0);
            if (bucketNodes.containsKey(bucketNodeId))
                bucketNodes.put(bucketNodeId, keys.get(bucketNodeId));
            bucketNodes.put(bucketNodeId, keys.get(bucketNodeId));
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
            if (representative == leaf.getID()) bucketNodes.add(peer.getID());
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

    // static ArrayList<Long> getChildren(LinkedHashMap<Long, RoutingTable>
    // peers,
    // long node) {
    //
    // }
}
