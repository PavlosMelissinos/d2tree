package d2tree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import d2tree.RoutingTable.Role;

public class DataExtractor {
    // public DataExtractor(){
    //
    // }
    static HashMap<Long, Long> getBucketNodes(List<D2TreeCore> peers) {
        HashMap<Long, Long> bucketNodes = new HashMap<Long, Long>();
        for (D2TreeCore peer : peers) {
            long leafId = peer.getRT().get(Role.REPRESENTATIVE);
            if (peer.isBucketNode()) bucketNodes.put(leafId, peer.id);
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
}
