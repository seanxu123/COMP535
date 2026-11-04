package socs.network.message;

import java.io.*;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

public class SOSPFPacket implements Serializable {

  //for inter-process communication
  public String srcProcessIP;
  public int srcProcessPort;

  //simulated IP address
  public String srcIP;
  public String dstIP;

  //common header
  // 0 - HELLO, 1 - LinkState Update, 2 - Application Message, 3 - Weight Update, 4 - LINK_DOWN, 5 - ROUTER_DOWN
  public short sospfType;
  public String routerID;

  //used by HELLO message to identify the sender of the message
  //e.g. when router A sends HELLO to its neighbor, it has to fill this field with its own
  //simulated IP address
  public String neighborID; //neighbor's simulated IP address

  //used by LSAUPDATE
  public Vector<LSA> lsaArray = null;

  //used by Application Message
  public String message; //user inputted message payload

  public int weight;

}
