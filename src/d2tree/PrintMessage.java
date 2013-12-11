/*
 * To change this template, choose Tools | Templates and open the template in
 * the editor.
 */

package d2tree;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import p2p.simulator.message.Message;
import p2p.simulator.message.MessageBody;

/**
 * 
 * @author Pavlos Melissinos
 */
public class PrintMessage extends MessageBody {
    private static final long serialVersionUID = -6662495188045778809L;

    // private boolean down;
    private long              initialNode;
    private int               msgType;
    public static String      logDir           = "D:\\logs\\";

    // public PrintMessage(boolean down, int msgType, long initialNode) {
    // this.down = down;
    public PrintMessage(int msgType, long initialNode) {
        this.initialNode = initialNode;
        this.msgType = msgType;
    }

    long getInitialNode() {
        return initialNode;
    }

    // public boolean goesDown(){
    // return this.down;
    // }
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
            System.out.println("Saving log to " + logFile);
            PrintWriter out = new PrintWriter(new FileWriter(logFile, true));

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
            String logFile, long initialNode) {
        try {
            System.out.println("Saving log to " + logFile);
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
}
