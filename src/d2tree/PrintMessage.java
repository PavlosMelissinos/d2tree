/*
 * To change this template, choose Tools | Templates and open the template in
 * the editor.
 */

package d2tree;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import p2p.simulator.message.Message;
import p2p.simulator.message.MessageBody;
import d2tree.RoutingTable.Role;

/**
 * 
 * @author Pavlos Melissinos
 */
public class PrintMessage extends MessageBody {

    // private boolean down;
    private long        initialNode;
    private int         msgType;
    static final String logDir      = "D:/logs/";
    static final String indexLogDir = logDir + "index/";
    // public static String logDir = "../logs/";
    static final String allLogFile  = logDir + "main.txt";
    static final String treeLogFile = logDir + "tree.txt";

    // static final String indexLogFile = logIndexDir + "tree.txt";

    private class IdKeyRangePair {
        private long   id;
        private double keyMin;
        private double keyMax;

        IdKeyRangePair(long id, double keyMin, double keyMax) {
            this.id = id;
            this.keyMin = keyMin;
            this.keyMax = keyMax;
        }
    };

    // public PrintMessage(boolean down, int msgType, long initialNode) {
    // this.down = down;
    public PrintMessage(int msgType, long initialNode) {
        this.initialNode = initialNode;
        this.msgType = msgType;
    }

    public long getInitialNode() {
        return initialNode;
    }

    public int getSourceType() {
        return this.msgType;
    }

    @Override
    public int getType() {
        return D2TreeMessageT.PRINT_MSG;
    }

    static public synchronized void print(Message msg, String printText,
            String logFile) {
        try {
            if (!logFile.equals(PrintMessage.logDir + "errors.txt") &&
                    !logFile.equals(PrintMessage.logDir + "conn-disconn.txt") &&
                    !logFile.equals(PrintMessage.logDir + "messages.txt")) {
                // System.out.println(logFile.substring(logFile.lastIndexOf('/')));
                System.out.println(logFile);
            }
            PrintWriter out = new PrintWriter(new FileWriter(
                    PrintMessage.logDir + logFile, true));

            // PrintMessage data = (PrintMessage) msg.getData();
            out.format("\n%s(MID = %d): %s",
                    D2TreeMessageT.toString(msg.getType()), msg.getMsgId(),
                    printText);
            out.close();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    static public synchronized void print(Message msg, String printText,
            String logFile, long initialNode, boolean stdOutFlag) {
        try {
            if (!logFile.equals(PrintMessage.logDir + "errors.txt") &&
                    stdOutFlag) {
                System.out.println(logFile);
            }
            PrintWriter out = new PrintWriter(new FileWriter(logFile, true));

            // PrintMessage data = (PrintMessage) msg.getData();
            out.format("\n%s(MID = %d) %d: %s",
                    D2TreeMessageT.toString(msg.getType()), msg.getMsgId(),
                    initialNode, printText);
            out.close();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    static ArrayList<LinkedHashMap<Long, ArrayList<Double>>> getPeerIdsByTreeLevel(
            HashMap<Long, RoutingTable> myPeers) {
        // group peers by tree level
        LinkedHashMap<Long, ArrayList<Double>> currentLevelNodes = new LinkedHashMap<Long, ArrayList<Double>>();
        LinkedHashMap<Long, ArrayList<Double>> nextLevelNodes = new LinkedHashMap<Long, ArrayList<Double>>();
        LinkedHashMap<Long, ArrayList<Double>> keys = D2Tree.getAllKeys();

        currentLevelNodes.put(1L, keys.get(1L));

        ArrayList<LinkedHashMap<Long, ArrayList<Double>>> allPeers = new ArrayList<LinkedHashMap<Long, ArrayList<Double>>>();
        while (!currentLevelNodes.isEmpty()) {
            allPeers.add(currentLevelNodes);
            for (Long peerId : currentLevelNodes.keySet()) {
                RoutingTable peerRT = myPeers.get(peerId);
                if (peerRT == null)
                    throw new IllegalArgumentException("Peer group " +
                            myPeers.keySet() + " does not contain peer " +
                            peerId + ".");
                // assert peerRT != null;
                if (!peerRT.isLeaf() && !peerRT.isBucketNode()) {
                    long leftChild = peerRT.get(Role.LEFT_CHILD);
                    long rightChild = peerRT.get(Role.RIGHT_CHILD);
                    if (leftChild != RoutingTable.DEF_VAL)
                        nextLevelNodes.put(leftChild, keys.get(leftChild));
                    if (rightChild != RoutingTable.DEF_VAL)
                        nextLevelNodes.put(rightChild, keys.get(rightChild));
                }
            }
            currentLevelNodes = new LinkedHashMap<Long, ArrayList<Double>>(
                    nextLevelNodes);
            nextLevelNodes.clear();
        }
        return allPeers;
    }

    static void printPBT(Message msg,
            LinkedHashMap<Long, RoutingTable> peerRTs, String logFile)
            throws IOException {

        ArrayList<LinkedHashMap<Long, ArrayList<Double>>> pbtPeers = PrintMessage
                .getPeerIdsByTreeLevel(peerRTs);
        for (LinkedHashMap<Long, ArrayList<Double>> peers : pbtPeers) {

            PrintWriter out = null;
            out = new PrintWriter(new FileWriter(logDir + logFile, true));
            out.print("\n\n");
            out.println(peers);
            out.close();

            out = new PrintWriter(new FileWriter(allLogFile, true));
            out.print("\n\n");
            out.println(peers);
            out.close();

            out = new PrintWriter(new FileWriter(indexLogDir + logFile, true));
            out.print("\n\n");
            out.println(peers);
            out.close();

            // for (Long peerId : peerIds) {
            //
            // RoutingTable peerRT = peerRTs.get(peerId);
            // if (peerRT == null) continue;
            // PrintWriter out = null;
            // out = new PrintWriter(new FileWriter(logFile, true));
            // out.format("\nId=%3d,", peerId);
            // peerRT.print(out);
            // out.close();
            //
            // out = new PrintWriter(new FileWriter(allLogFile, true));
            // out.format("\nMID=%3d, Id=%3d,", msg.getMsgId(), peerId);
            // peerRT.print(out);
            // out.close();
            //
            // out = new PrintWriter(new FileWriter(allLogFile, true));
            // out.format("\nMID=%3d, Id=%3d,", msg.getMsgId(), peerId);
            // peerRT.print(out);
            // out.close();
            // }
        }
        if (pbtPeers.isEmpty()) throw new IllegalStateException();

    }

    static void printBuckets(LinkedHashMap<Long, RoutingTable> peerRTs,
            String logFile) throws IOException {

        ArrayList<LinkedHashMap<Long, ArrayList<Double>>> pbtPeers = PrintMessage
                .getPeerIdsByTreeLevel(peerRTs);
        LinkedHashMap<Long, ArrayList<Long>> bucketIds = DataExtractor
                .getBucketNodes(peerRTs);

        LinkedHashMap<Long, LinkedHashMap<Long, ArrayList<Double>>> properBucketIds = new LinkedHashMap<Long, LinkedHashMap<Long, ArrayList<Double>>>();
        LinkedHashMap<Long, ArrayList<Double>> leaves = pbtPeers.get(pbtPeers
                .size() - 1);
        for (Long leaf : leaves.keySet()) {
            properBucketIds.put(leaf,
                    DataExtractor.getOrderedBucketNodes(peerRTs, leaf));
        }

        PrintWriter out = new PrintWriter(
                new FileWriter(logDir + logFile, true));
        PrintWriter out1 = new PrintWriter(new FileWriter(allLogFile, true));
        PrintWriter out2 = new PrintWriter(new FileWriter(treeLogFile, true));
        PrintWriter out3 = new PrintWriter(new FileWriter(
                indexLogDir + logFile, true));

        int counter = 0;
        for (Long leaf : leaves.keySet()) {
            int properBucketSize = properBucketIds.get(leaf).size();
            String properText = String
                    .format("\nPrinting Bucket of %d (navigation via bucket nodes' routing tables, starting from %d), size = %d \n%s",
                            leaf, leaf, properBucketSize,
                            properBucketIds.get(leaf));
            int bucketSize = bucketIds.containsKey(leaf) ? bucketIds.get(leaf)
                    .size() : 0;
            String text = String
                    .format("\nPrinting Bucket of %d (show all nodes with %d as a representatives), size = %d \n%s",
                            leaf, leaf, bucketSize, bucketIds.get(leaf));
            if (counter % 2 == 1) {
                properText += "\n";
                text += "\n";
            }
            out.format(text);
            out.format(properText);

            out1.format(text);
            out1.format(properText);

            out2.format(text);
            out2.format(properText);
            counter++;
        }
        out.close();
        out1.close();
        out2.close();
        // for (Long leafId : bucketIds.keySet()) {
        //
        // ArrayList<Long> bucket = bucketIds.get(leafId);
        // out.format("\nId=%3d,", leafId);
        // out.print(bucket);
        // out.close();
        //
        // out1 = new PrintWriter(new FileWriter(allLogFile, true));
        // out.format("\nMID=%3d, Id=%3d,", msg.getMsgId(), peerId);
        // peerRT.print(out);
        // out.close();
        //
        // out2 = new PrintWriter(new FileWriter(treeLogFile, true));
        // out.format("\nMID=%3d, Id=%3d,", msg.getMsgId(), peerId);
        // peerRT.print(out);
        // out.close();
        // }
    }

    static void printTreeByIndex(List<D2TreeCore> peers, Message msg,
            String logFile) throws IOException {
        PrintMessage data = (PrintMessage) msg.getData();
        long id = msg.getDestinationId();
        if (id == msg.getSourceId()) {
            System.out.println(logFile);
            // TODO Could the removal of a peer (contract) cause problems in the
            // loop? - test this case
            for (int index = 0; index < peers.size(); index++) {
                D2TreeCore peer = peers.get(index);
                RoutingTable peerRT = peer.getRT();
                PrintWriter out = null;
                try {
                    out = new PrintWriter(
                            new FileWriter(logDir + logFile, true));
                    out.format("\nMID=%3d, Id=%3d,", msg.getMsgId(),
                            peer.getID());
                    peerRT.print(out);
                    out.close();

                    out = new PrintWriter(new FileWriter(allLogFile, true));
                    out.format("\nMID=%3d, Id=%3d,", msg.getMsgId(),
                            peer.getID());
                    peerRT.print(out);
                    out.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        if (data.getSourceType() == D2TreeMessageT.JOIN_REQ ||
                data.getSourceType() == D2TreeMessageT.JOIN_RES) return;
        PrintWriter out2 = null;
        try {
            out2 = new PrintWriter(new FileWriter(treeLogFile, true));
            for (int index = 0; index < peers.size(); index++) {
                D2TreeCore peer = peers.get(index);
                String msgType = peer.isRoot() ? D2TreeMessageT.toString(data
                        .getSourceType()) + "\n" : "";
                out2.format("\n%s MID=%5d, Id=%3d,", msgType, msg.getMsgId(),
                        peer.getID());
                peer.getRT().print(out2);
                if (data.getSourceType() == D2TreeMessageT.PRINT_ERR_MSG &&
                        peer.getID() == id) {
                    out2.format(" <-- DISCREPANCY DETECTED");
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        out2.close();
        // PrintMessage data = (PrintMessage) msg.getData();
        //
        // PrintWriter indieLog = new PrintWriter(new FileWriter(logFile,
        // true));
        // PrintWriter allLog = new PrintWriter(new FileWriter(allLogFile,
        // true));
        // PrintWriter treeLog = new PrintWriter(new FileWriter(treeLogFile,
        // true));
        // for (int index = 0; index < peers.size(); index++) {
        // D2TreeCore peer = peers.get(index);
        // if (peer == null) continue;
        // RoutingTable peerRT = peer.getRT();
        //
        // indieLog.format("\nId=%3d,", peer.id);
        // peerRT.print(indieLog);
        //
        // allLog.format("\nMID=%3d, Id=%3d,", msg.getMsgId(), peer.id);
        // peerRT.print(allLog);
        //
        // String msgType = peer.isRoot() ? D2TreeMessageT.toString(data
        // .getSourceType()) + " \n" : "";
        // treeLog.format("\n%sMID=%5d, Id=%3d,", msgType, msg.getMsgId(),
        // peer.id);
        // peerRT.print(treeLog);
        // // if (data.getSourceType() == D2TreeMessageT.PRINT_ERR_MSG &&
        // // peer.id == id) {
        // // out3.format(" <-- DISCREPANCY DETECTED");
        // // out4.format(" <-- DISCREPANCY DETECTED");
        // // }
        // }
    }

    static void serializePeers(long msgId, String objectFile,
            LinkedHashMap<Long, RoutingTable> routingTables) {

        try {
            ObjectOutputStream oos = new ObjectOutputStream(
                    new FileOutputStream(objectFile));
            oos.writeLong(msgId);
            oos.writeObject(routingTables);
            oos.close();
        }
        catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static final long serialVersionUID = -6662495188045778809L;
}
