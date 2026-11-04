package socs.network.node;

import socs.network.message.LSA;
import socs.network.message.LinkDescription;
import socs.network.message.SOSPFPacket;
import socs.network.util.Configuration;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;


public class Router {

  protected LinkStateDatabase lsd;

  RouterDescription rd = new RouterDescription();

  // assuming that all routers are with 4 ports
  Link[] ports = new Link[4];

  private ServerSocket serverSocket;
  private final BufferedReader consoleReader = new BufferedReader(new InputStreamReader(System.in));

  private volatile boolean started = false;
  public final AtomicInteger weightUpdateSent = new AtomicInteger(0);
  public final AtomicInteger weightUpdateReceived = new AtomicInteger(0);
  public final AtomicInteger linkDownSent = new AtomicInteger(0);
  public final AtomicInteger linkDownReceived = new AtomicInteger(0);

  // Buffer for concurrent attach requests
  private static class AttachRequest {
    final SOSPFPacket packet;
    final Socket socket;
    final ObjectInputStream in;
    final ObjectOutputStream out;
    final CompletableFuture<Boolean> answerFuture = new CompletableFuture<>();

    AttachRequest(SOSPFPacket packet, Socket socket, ObjectInputStream in, ObjectOutputStream out) {
      this.packet = packet;
      this.socket = socket;
      this.in = in;
      this.out = out;
    }
  }

  private final BlockingQueue<AttachRequest> attachQueue = new LinkedBlockingQueue<>();
  private final Object queueLock = new Object();
  private AttachRequest activeAttachRequest = null;

  private int currentSeqNumber = 0;

  // Message counters
  public final AtomicInteger helloSent = new AtomicInteger(0);
  public final AtomicInteger helloReceived = new AtomicInteger(0);
  public final AtomicInteger lsaUpdateSent = new AtomicInteger(0);
  public final AtomicInteger lsaUpdateReceived = new AtomicInteger(0);
  public final AtomicInteger appMessageSent = new AtomicInteger(0);
  public final AtomicInteger appMessageReceived = new AtomicInteger(0);

  public Router(Configuration config) {
    rd.simulatedIPAddress = config.getString("socs.network.router.ip");
    lsd = new LinkStateDatabase(rd);

    // set up server socket on any available port
    try {
      serverSocket = new ServerSocket(0);
      rd.processIPAddress = InetAddress.getLocalHost().getHostAddress();
      rd.processPortNumber = serverSocket.getLocalPort();
    } catch (Exception e) {
      System.err.println("Failed to create server socket: " + e.getMessage());
      System.exit(1);
    }

    // print router info so user can run attach from other terminals
    System.out.println("========================================");
    System.out.println("Process IP  : " + rd.processIPAddress);
    System.out.println("Process Port: " + rd.processPortNumber);
    System.out.println("Simulated IP: " + rd.simulatedIPAddress);
    System.out.println("========================================");

    // start listening for incoming connections in background
    Thread listenerThread = new Thread(() -> requestHandler());
    listenerThread.setDaemon(true);
    listenerThread.start();
  }

  /**
   * output the shortest path to the given destination ip
   * <p/>
   * format: source ip address  -> ip address -> ... -> destination ip
   *
   * @param destinationIP the ip adderss of the destination simulated router
   */
  private void processDetect(String destinationIP) {
    String path = lsd.getShortestPath(destinationIP);
    if (path == null) {
      System.out.println("Path not found");
    } else {
      System.out.println("Path found: " + path);
    }
  }

  /**
   * disconnect with the router identified by the given port number.
   * Notice: this command should trigger the synchronization of database
   *
   * @param portNumber the port number which the link attaches at
   */
  private void processDisconnect(short portNumber) {
    if (portNumber < 0 || portNumber >= ports.length) {
      System.out.println("Invalid port number: must be between 0 and " + (ports.length - 1));
      return;
    }

    Link link;
    synchronized (ports) {
      link = ports[portNumber];
      if (link == null) {
        System.out.println("Port " + portNumber + " is not in use.");
        return;
      }
      ports[portNumber] = null;
    }

    // Explicit notification to neighbor on this link
    sendLinkDown(link, (short) 4);

    try {
      link.socket.close();
    } catch (Exception ignored) {}
    link.router2.status = null;

    System.out.println("Port " + portNumber + " disconnected");
    System.out.println("Broadcasting LSAUPDATE");

    synchronized (this) {
      updateLocalLSA();
      broadcastLSAUpdate(lsd.getAllLSAs(), null);
    }
  }

  /**
   * attach the link to the remote router, which is identified by the given simulated ip;
   * to establish the connection via socket, you need to indentify the process IP and process Port;
   * additionally, weight is the cost to transmitting data through the link
   * <p/>
   * NOTE: this command should not trigger link database synchronization
   */
  private void processAttach(String processIP, int processPort,
                             String simulatedIP, short weight) {
    attachLink(processIP, processPort, simulatedIP, weight, true);
  }

  private Link attachLink(String processIP, int processPort, String simulatedIP,
                          short weight, boolean announceAccepted) {
    // Check for duplicate attachment and find a free port atomically
    int freePort = -1;
    synchronized (ports) {
      for (Link link : ports) {
        if (link != null && link.router2.simulatedIPAddress.equals(simulatedIP)) {
          System.out.println("Already attached to " + simulatedIP);
          return null;
        }
      }
      for (int i = 0; i < ports.length; i++) {
        if (ports[i] == null) {
          freePort = i;
          break;
        }
      }
    }
    if (freePort == -1) {
      System.out.println("All ports are occupied, cannot attach.");
      return null;
    }

    try {
      // Open TCP connection to the remote router's server socket
      Socket socket = new Socket(processIP, processPort);
      ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
      ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

      // Build and send HELLO packet to request attachment
      SOSPFPacket hello = makeHelloPacket(simulatedIP, weight);
      out.writeObject(hello);
      out.flush();
      helloSent.incrementAndGet();

      // Wait for accept/reject response
      SOSPFPacket response = (SOSPFPacket) in.readObject();

      if (response.sospfType == 0) {
        helloReceived.incrementAndGet();
        // Accepted — create the link and store it
        RouterDescription remoteRd = new RouterDescription();
        remoteRd.processIPAddress = processIP;
        remoteRd.processPortNumber = processPort;
        remoteRd.simulatedIPAddress = simulatedIP;

        final Link link = new Link(rd, remoteRd, weight, socket, out, in);
        synchronized (ports) {
          ports[freePort] = link;
        }
        if (announceAccepted) {
          System.out.println("Your attach request has been accepted;");
        }

        Thread listener = new Thread(() -> listenOnLink(link));
        listener.setDaemon(true);
        listener.start();
        return link;
      } else {
        // Rejected
        System.out.println("Your attach request has been rejected;");
        socket.close();
        return null;
      }
    } catch (Exception e) {
      System.err.println("Failed to attach to " + simulatedIP + ": " + e.getMessage());
      return null;
    }
  }

  private SOSPFPacket makeHelloPacket(String dstIP, int weight) {
    SOSPFPacket pkt = new SOSPFPacket();
    pkt.srcProcessIP = rd.processIPAddress;
    pkt.srcProcessPort = rd.processPortNumber;
    pkt.srcIP = rd.simulatedIPAddress;
    pkt.dstIP = dstIP;
    pkt.sospfType = 0;
    pkt.routerID = rd.simulatedIPAddress;
    pkt.neighborID = rd.simulatedIPAddress;
    pkt.weight = weight;
    return pkt;
  }

  private SOSPFPacket makeRejectPacket() {
    SOSPFPacket pkt = new SOSPFPacket();
    pkt.sospfType = -1;
    return pkt;
  }

  /**
   * process request from the remote router.
   * For example: when router2 tries to attach router1. Router1 can decide whether it will accept this request.
   * The intuition is that if router2 is an unknown/anomaly router, it is always safe to reject the attached request from router2.
   *
   * Runs in a background daemon thread. Loops forever accepting incoming TCP connections
   * and spawns a new thread to handle each one.
   */
  private void requestHandler() {
    while (true) {
      try {
        Socket clientSocket = serverSocket.accept();
        Thread handler = new Thread(() -> handleIncomingConnection(clientSocket));
        handler.setDaemon(true);
        handler.start();
      } catch (Exception e) {
        System.err.println("Error accepting connection: " + e.getMessage());
      }
    }
  }

  private void handleIncomingConnection(Socket clientSocket) {
    try {
      ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
      ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());

      SOSPFPacket packet = (SOSPFPacket) in.readObject();
      if (packet.sospfType == 0) {
        helloReceived.incrementAndGet();
        handleHello(packet, clientSocket, in, out);
      }
    } catch (Exception e) {
      System.err.println("Error handling incoming connection: " + e.getMessage());
      try { clientSocket.close(); } catch (Exception ignored) {}
    }
  }

  private void handleHello(SOSPFPacket packet, Socket clientSocket, ObjectInputStream in, ObjectOutputStream out) throws Exception {
    String senderSimIP = packet.neighborID;

    // Check if link already exists
    boolean alreadyAttached;
    synchronized (ports) {
      alreadyAttached = false;
      for (Link link : ports) {
        if (link != null && link.router2.simulatedIPAddress.equals(senderSimIP)) {
          alreadyAttached = true;
          break;
        }
      }
    }

    if (alreadyAttached) {
      // Already attached: HELLO belongs to start handshake
      return;
    }

    AttachRequest req = new AttachRequest(packet, clientSocket, in, out);
    synchronized (queueLock) {
      if ((activeAttachRequest != null && activeAttachRequest.packet.neighborID.equals(senderSimIP)) ||
          attachQueue.stream().anyMatch(r -> r.packet.neighborID.equals(senderSimIP))) {
        out.writeObject(makeRejectPacket());
        out.flush();
        clientSocket.close();
        return;
      }

      if (activeAttachRequest == null) {
        activeAttachRequest = req;
        promptAttachRequest(req);
      } else {
        attachQueue.offer(req);
      }
    }

    boolean accepted;
    try {
      accepted = req.answerFuture.get();
    } catch (Exception e) {
      accepted = false;
    }

    if (accepted) {
      // Find a free port and store the link atomically
      int freePort = -1;
      Link newLink = null;
      synchronized (ports) {
        for (int i = 0; i < ports.length; i++) {
          if (ports[i] == null) {
            freePort = i;
            break;
          }
        }
        if (freePort != -1) {
          RouterDescription remoteRd = new RouterDescription();
          remoteRd.processIPAddress = packet.srcProcessIP;
          remoteRd.processPortNumber = packet.srcProcessPort;
          remoteRd.simulatedIPAddress = senderSimIP;
          newLink = new Link(rd, remoteRd, packet.weight, clientSocket, out, in);
          ports[freePort] = newLink;
        }
      }
      if (freePort == -1) {
        System.out.println("All ports are occupied, rejecting.");
        out.writeObject(makeRejectPacket());
        out.flush();
        clientSocket.close();
      } else {
        System.out.println("You accepted the attach request from " + senderSimIP + ";");
        out.writeObject(makeHelloPacket(senderSimIP, packet.weight));
        out.flush();
        helloSent.incrementAndGet();
        final Link link = newLink;
        Thread listener = new Thread(() -> listenOnLink(link));
        listener.setDaemon(true);
        listener.start();
      }
    } else {
      System.out.println("You rejected the attach request;");
      out.writeObject(makeRejectPacket());
      out.flush();
      clientSocket.close();
    }
  }

  private void promptAttachRequest(AttachRequest req) {
    System.out.println("received HELLO from " + req.packet.neighborID + ";");
    System.out.println("Do you accept this request? (Y/N)");
  }

  private void listenOnLink(Link link){
    try {
      while (true) {
        SOSPFPacket packet = (SOSPFPacket) link.ois.readObject();
        try {
          if (packet.sospfType == 0) {
            helloReceived.incrementAndGet();
            handleHelloOnLink(link, packet);
          } else if (packet.sospfType == 1) {
            lsaUpdateReceived.incrementAndGet();
            handleLSAUpdate(link, packet);
          } else if (packet.sospfType == 2) {
            appMessageReceived.incrementAndGet();
            handleApplicationMessage(packet);
          } else if (packet.sospfType == 3){
            weightUpdateReceived.incrementAndGet();
            handleWeightUpdate(link, packet);
          } else if (packet.sospfType == 4 || packet.sospfType == 5) {
            linkDownReceived.incrementAndGet();
            handleLinkFailure(link);
            break;
          }
        } catch (Exception e) {
          // a bad packet or handler bug must not kill this link's listener
          System.err.println("Error handling packet: " + e);
        }
      }
    } catch (Exception e) {
      // socket closed or link disconnected (unannounced failure or unexpected closure)
      handleLinkFailure(link);
    }
  }

  private synchronized void handleHelloOnLink(Link link, SOSPFPacket packet){
    RouterDescription neighbor = link.router2;
    System.out.println("received HELLO from " + packet.srcIP + ";");

    if (neighbor.status == null){
      if (link.startedByUs){
        neighbor.status = RouterStatus.TWO_WAY;
        System.out.println("set " + packet.srcIP + " STATE to TWO_WAY;");
        sendHello(link);
        updateLocalLSA();
        broadcastLSAUpdate(lsd.getAllLSAs(), null);
      } else {
        neighbor.status = RouterStatus.INIT;
        System.out.println("set " + packet.srcIP + " STATE to INIT;");
        sendHello(link);
      }
    } else if (neighbor.status == RouterStatus.INIT) {
      neighbor.status = RouterStatus.TWO_WAY;
      System.out.println("set " + packet.srcIP + " STATE to TWO_WAY;");
      updateLocalLSA();
      broadcastLSAUpdate(lsd.getAllLSAs(), null);
    }
  }

  private void sendHello(Link link){
    try {
      synchronized (link) {
        link.oos.reset();
        link.oos.writeObject(makeHelloPacket(link.router2.simulatedIPAddress, link.weight));
        link.oos.flush();
      }
      helloSent.incrementAndGet();
    } catch (Exception e){
      System.err.println("Failed to send HELLO to " + link.router2.simulatedIPAddress);
    }
  }

  /**
   * broadcast Hello to neighbors and synchronize link state database
   */
  private void processStart() {
    started = true;
    boolean hasTwoWay = false;

    synchronized (ports) {
      for (Link link : ports) {
        if (link != null){
          link.startedByUs = true;
          sendHello(link);
          if (link.router2.status == RouterStatus.TWO_WAY) {
            hasTwoWay = true;
          }
        }
      }
    }

    if (hasTwoWay) {
      updateLocalLSA();
      broadcastLSAUpdate(lsd.getAllLSAs(), null);
    }
  }

  // Rebuild local LSA with current TWO_WAY neighbors and update LSDB
  private synchronized void updateLocalLSA() {
    currentSeqNumber++;
    LSA lsa = new LSA();
    lsa.linkStateID = rd.simulatedIPAddress;
    lsa.lsaSeqNumber = currentSeqNumber;

    synchronized (ports) {
      for (int i = 0; i < ports.length; i++) {
        Link link = ports[i];
        if (link != null && link.router2.status == RouterStatus.TWO_WAY) {
          LinkDescription ld = new LinkDescription();
          ld.linkID = link.router2.simulatedIPAddress;
          ld.portNum = i;
          ld.tosMetrics = 0;
          ld.weight = link.weight;
          lsa.links.add(ld);
        }
      }
    }
    lsd.putLSA(lsa);
  }

  private void sendLSAUpdate(Link link, Vector<LSA> lsaList) {
    try {
      SOSPFPacket pkt = new SOSPFPacket();
      pkt.srcProcessIP = rd.processIPAddress;
      pkt.srcProcessPort = rd.processPortNumber;
      pkt.srcIP = rd.simulatedIPAddress;
      pkt.dstIP = link.router2.simulatedIPAddress;
      pkt.sospfType = 1;
      pkt.routerID = rd.simulatedIPAddress;
      pkt.lsaArray = lsaList;

      synchronized (link) {
        link.oos.reset();
        link.oos.writeObject(pkt);
        link.oos.flush();
      }
      lsaUpdateSent.incrementAndGet();
    } catch (Exception e) {
      System.err.println("Failed to send LSAUPDATE to " + link.router2.simulatedIPAddress + ": " + e.getMessage());
    }
  }

  // Flood LSAs to all TWO_WAY neighbors except excludeLink
  private void broadcastLSAUpdate(Vector<LSA> lsaList, Link excludeLink) {
    if (lsaList == null || lsaList.isEmpty()) {
      return;
    }
    synchronized (ports) {
      for (Link link : ports) {
        if (link != null && link.router2.status == RouterStatus.TWO_WAY && link != excludeLink) {
          sendLSAUpdate(link, lsaList);
        }
      }
    }
  }

  // Process received LSA update and forward newer LSAs
  private void handleLSAUpdate(Link incomingLink, SOSPFPacket packet) {
    if (packet.lsaArray == null || packet.lsaArray.isEmpty()) {
      return;
    }
    Vector<LSA> newerLSAs = new Vector<>();
    for (LSA lsa : packet.lsaArray) {
      if (lsd.updateLSA(lsa)) {
        newerLSAs.add(lsa);
      }
    }
    if (!newerLSAs.isEmpty()) {
      broadcastLSAUpdate(newerLSAs, incomingLink);
    }
  }

  /**
   * attach the link to the remote router, which is identified by the given simulated ip;
   * to establish the connection via socket, you need to indentify the process IP and process Port;
   * additionally, weight is the cost to transmitting data through the link
   * <p/>
   * This command does trigger the link database synchronization
   */
  private void processConnect(String processIP, int processPort,
                              String simulatedIP, short weight) {
    if (!started) {
      System.out.println("Cannot connect: run start first.");
      return;
    }
    Link link = attachLink(processIP, processPort, simulatedIP, weight, false);
    if (link == null) {
      return; // attachLink already printed the reason
    }

    // same as start, but for this link only: we initiate the handshake
    synchronized (this) {
      link.startedByUs = true;
    }
    sendHello(link);

    // The handshake completes on the listener thread; wait (up to 5s) for TWO_WAY.
    // Checking under the router lock also guarantees the LSAUPDATE broadcast has been sent.
    boolean established = false;
    long deadline = System.currentTimeMillis() + 5000;
    while (System.currentTimeMillis() < deadline) {
      synchronized (this) {
        established = link.router2.status == RouterStatus.TWO_WAY;
      }
      if (established) break;
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    if (established) {
      System.out.println("Link established with " + simulatedIP);
      System.out.println("Broadcasting LSAUPDATE.");
    } else {
      System.out.println("Attached to " + simulatedIP + " but the handshake did not complete.");
    }
  }

  /**
   * output the neighbors of the routers
   */
  private void processNeighbors() {
    synchronized (ports) {
      boolean hasNeighbors = false;
      for (Link link : ports) {
        if (link != null && link.router2.status == RouterStatus.TWO_WAY) {
          System.out.println(link.router2.simulatedIPAddress);
          hasNeighbors = true;
        }
      }

      if (!hasNeighbors) {
        System.out.println("No neighbors found.");
      }
    }
  }

  private void processStats() {
    System.out.println("HELLO sent: " + helloSent.get());
    System.out.println("HELLO received: " + helloReceived.get());
    System.out.println("LSAUPDATE sent: " + lsaUpdateSent.get());
    System.out.println("LSAUPDATE received: " + lsaUpdateReceived.get());
    System.out.println("APP_MESSAGE sent: " + appMessageSent.get());
    System.out.println("APP_MESSAGE received: " + appMessageReceived.get());
    System.out.println("WEIGHT_UPDATE sent: " + weightUpdateSent.get());
    System.out.println("WEIGHT_UPDATE received: " + weightUpdateReceived.get());
    System.out.println("LINK_DOWN sent: " + linkDownSent.get());
    System.out.println("LINK_DOWN received: " + linkDownReceived.get());
  }

  /**
   * disconnect with all neighbors and quit the program
   */
  private void processQuit() {
    System.out.println("Broadcasting final LSAUPDATE");

    List<Link> activeLinks = new ArrayList<>();
    synchronized (ports) {
      for (int i = 0; i < ports.length; i++) {
        if (ports[i] != null) {
          if (ports[i].router2.status == RouterStatus.TWO_WAY) {
            activeLinks.add(ports[i]);
          }
          ports[i] = null;
        }
      }
    }

    // Rebuild self-originated LSA with empty links list and incremented sequence number
    LSA finalLSA;
    synchronized (this) {
      currentSeqNumber++;
      finalLSA = new LSA();
      finalLSA.linkStateID = rd.simulatedIPAddress;
      finalLSA.lsaSeqNumber = currentSeqNumber;
      // finalLSA.links is empty (no active links)
      lsd.putLSA(finalLSA);
    }

    // Send final LSAUPDATE containing the empty LSA to each active neighbor
    Vector<LSA> finalLSAList = new Vector<>();
    finalLSAList.add(finalLSA);
    for (Link link : activeLinks) {
      sendLSAUpdate(link, finalLSAList);
    }

    // Send explicit ROUTER_DOWN notification (type 5) to each active neighbor
    for (Link link : activeLinks) {
      sendLinkDown(link, (short) 5);
    }

    // Small delay to ensure packets flush out across network before closing sockets
    try {
      Thread.sleep(150);
    } catch (InterruptedException ignored) {}

    // Close all neighbor sockets
    for (Link link : activeLinks) {
      try {
        link.socket.close();
      } catch (Exception ignored) {}
      link.router2.status = null;
    }

    // Close listening server socket
    try {
      if (serverSocket != null) {
        serverSocket.close();
      }
    } catch (Exception ignored) {}

    System.out.println("Router shutting down.");
    System.exit(0);
  }

  /**
   * update the weight of an attached link
   */
  private void updateWeight(String processIP, int processPort,
                             String simulatedIP, short weight){

  }

  /**
   * update the weight of a specific port.
   * This change should trigger synchronization of the Link State Database by sending
   * a Link State Advertisement (LSA) update to all neighboring routers in the topology.
   *
   * @param portNumber the port number (0-3) to update
   * @param newWeight the new weight/cost for the link attached to this port
   */
  private void processUpdate(short portNumber, short newWeight) {
    if (portNumber < 0 || portNumber >= ports.length) {
      System.out.println("Invalid port number: must be between 0 and " + (ports.length - 1));
      return;
    }
    if (newWeight < 0) {
      System.out.println("Invalid weight: must not be negative.");
      return;
    }

    Link link;
    synchronized (ports) {
      link = ports[portNumber];
      if (link == null) {
        System.out.println("Port " + portNumber + " is not in use.");
        return;
      }
      link.weight = newWeight;
    }

    sendWeightUpdate(link, newWeight);   // remote end updates its own Link and LSA
    System.out.println("Link weight updated");

    synchronized (this) {                // rebuild LSA and flood as one step
      updateLocalLSA();
      System.out.println("Broadcasting LSAUPDATE");
      broadcastLSAUpdate(lsd.getAllLSAs(), null);
    }
  }

  // Tells the neighbor on this link about a new weight (sospfType = 3, WEIGHT_UPDATE)
  private void sendWeightUpdate(Link link, int newWeight) {
    try {
      SOSPFPacket pkt = new SOSPFPacket();
      pkt.srcProcessIP = rd.processIPAddress;
      pkt.srcProcessPort = rd.processPortNumber;
      pkt.srcIP = rd.simulatedIPAddress;
      pkt.dstIP = link.router2.simulatedIPAddress;
      pkt.sospfType = 3;
      pkt.routerID = rd.simulatedIPAddress;
      pkt.weight = newWeight;

      synchronized (link) {
        link.oos.reset();
        link.oos.writeObject(pkt);
        link.oos.flush();
      }
      weightUpdateSent.incrementAndGet();
    } catch (Exception e) {
      System.err.println("Failed to send WEIGHT_UPDATE to "
              + link.router2.simulatedIPAddress + ": " + e.getMessage());
    }
  }

  // The neighbor changed the weight of our shared link: mirror it, then advertise our new LSA
  private synchronized void handleWeightUpdate(Link link, SOSPFPacket packet) {
    synchronized (ports) {
      link.weight = packet.weight;
    }
    System.out.println("Link weight with " + packet.srcIP + " updated to " + packet.weight);
    updateLocalLSA();
    broadcastLSAUpdate(lsd.getAllLSAs(), null);
  }

  // Tells the neighbor on this link that the link or router is going down
  // (sospfType = 4 for LINK_DOWN, 5 for ROUTER_DOWN)
  private void sendLinkDown(Link link, short type) {
    try {
      SOSPFPacket pkt = new SOSPFPacket();
      pkt.srcProcessIP = rd.processIPAddress;
      pkt.srcProcessPort = rd.processPortNumber;
      pkt.srcIP = rd.simulatedIPAddress;
      pkt.dstIP = link.router2.simulatedIPAddress;
      pkt.sospfType = type;
      pkt.routerID = rd.simulatedIPAddress;

      synchronized (link) {
        link.oos.reset();
        link.oos.writeObject(pkt);
        link.oos.flush();
      }
      linkDownSent.incrementAndGet();
    } catch (Exception e) {
      // Remote end will detect socket closure
    }
  }

  /**
   * Idempotently handles link failure (explicit LINK_DOWN/ROUTER_DOWN or socket closure).
   * Ensures that duplicate or overlapping failure notifications are processed exactly once.
   */
  private void handleLinkFailure(Link link) {
    if (link == null) return;

    boolean removed = false;
    synchronized (ports) {
      for (int i = 0; i < ports.length; i++) {
        if (ports[i] == link) {
          ports[i] = null;
          removed = true;
          break;
        }
      }
    }

    if (!removed) {
      // Already cleaned up; idempotent guard against duplicate events
      return;
    }

    try {
      link.socket.close();
    } catch (Exception ignored) {}
    link.router2.status = null;

    synchronized (this) {
      updateLocalLSA();
      broadcastLSAUpdate(lsd.getAllLSAs(), null);
    }
  }

  /**
   * send an application-level message from this router to the destination router.
   * The message must be forwarded hop-by-hop according to the current shortest path.
   * <p/>
   * When you run send, the window of the router where you run the command should print:
   * "Sending message to <Destination IP>"
   * <p/>
   * For each intermediate router on the shortest path (excluding the source and destination),
   * the router window should print:
   * "Forwarding packet from <Source IP> to <Destination IP>"
   * <p/>
   * When the destination router receives the message, the router window should print:
   * "Received message from <Source IP>:"
   * "<Message>"
   *
   * @param destinationIP the simulated IP address of the destination router
   * @param message the message content to send
   */
  private void processSend(String destinationIP, String message) {
    if (destinationIP.equals(rd.simulatedIPAddress)) {
      System.out.println("Cannot send a message to this router itself.");
      return;
    }
    String nextHop = lsd.getNextHop(destinationIP);
    if (nextHop == null) {
      System.out.println("Path not found");
      return;
    }

    System.out.println("Sending message to " + destinationIP);
    SOSPFPacket pkt = new SOSPFPacket();
    pkt.srcProcessIP = rd.processIPAddress;
    pkt.srcProcessPort = rd.processPortNumber;
    pkt.srcIP = rd.simulatedIPAddress;
    pkt.dstIP = destinationIP;
    pkt.sospfType = 2;
    pkt.routerID = rd.simulatedIPAddress;
    pkt.message = message;
    forwardAppMessage(pkt, nextHop);
  }

  private void forwardAppMessage(SOSPFPacket pkt, String nextHop) {
    Link out = null;
    synchronized (ports) {
      for (Link link : ports) {
        if (link != null && link.router2.status == RouterStatus.TWO_WAY
                && link.router2.simulatedIPAddress.equals(nextHop)) {
          out = link;
          break;
        }
      }
    }
    if (out == null) {
      System.out.println("No active link to next hop " + nextHop + "; message dropped.");
      return;
    }
    try {
      synchronized (out) {
        out.oos.reset();
        out.oos.writeObject(pkt);
        out.oos.flush();
      }
      appMessageSent.incrementAndGet();
    } catch (Exception e) {
      System.err.println("Failed to forward message to " + nextHop + ": " + e.getMessage());
    }
  }

  /**
   * handle incoming application message packet.
   * This method should be called when a router receives a SOSPFPacket with sospfType = 2 (Application Message).
   * <p/>
   * If this router is the destination (packet.dstIP equals this router's simulatedIPAddress):
   * - Print "Received message from <Source IP>:"
   * - Print the message content
   * <p/>
   * If this router is an intermediate router:
   * - Print "Forwarding packet from <Source IP> to <Destination IP>"
   * - Forward the packet to the next hop on the shortest path to the destination
   * - Do NOT print or inspect the message payload
   *
   * @param packet the received application message packet
   */
  private void handleApplicationMessage(SOSPFPacket packet) {
    if (packet.dstIP.equals(rd.simulatedIPAddress)) {
      System.out.println("Received message from " + packet.srcIP + ";");
      System.out.println("Message: " + packet.message);
      return;
    }

    String nextHop = lsd.getNextHop(packet.dstIP);
    if (nextHop == null) {
      System.out.println("No route to " + packet.dstIP + "; message dropped.");
      return;
    }
    System.out.println("Forwarding message from " + packet.srcIP + " to " + packet.dstIP);
    System.out.println("Next hop: " + nextHop);
    forwardAppMessage(packet, nextHop);
  }

  public void terminal() {
    try {
      String command;
      while (true) {
        command = consoleReader.readLine();
        if (command == null) break;
        command = command.trim();

        // Handle pending attach request if one is waiting
        AttachRequest reqToAnswer = null;
        synchronized (queueLock) {
          if (activeAttachRequest != null) {
            reqToAnswer = activeAttachRequest;
            activeAttachRequest = attachQueue.poll();
            if (activeAttachRequest != null) {
              promptAttachRequest(activeAttachRequest);
            }
          }
        }

        if (reqToAnswer != null) {
          boolean accepted = command.equalsIgnoreCase("Y");
          reqToAnswer.answerFuture.complete(accepted);
          continue;
        }

        if (command.isEmpty()) {
          continue;
        }

        try {
          if (command.startsWith("detect ")) {
            String[] cmdLine = command.split(" ");
            processDetect(cmdLine[1]);
          } else if (command.startsWith("disconnect ") || command.equals("disconnect")) {
            String[] cmdLine = command.split(" ");
            if (cmdLine.length >= 2) {
              processDisconnect(Short.parseShort(cmdLine[1]));
            } else {
              System.out.println("Usage: disconnect [port_number]");
            }
          } else if (command.startsWith("quit")) {
            processQuit();
          } else if (command.startsWith("attach ")) {
            String[] cmdLine = command.split(" ");
            processAttach(cmdLine[1], Integer.parseInt(cmdLine[2]),
                    cmdLine[3], Short.parseShort(cmdLine[4]));
          } else if (command.equals("start")) {
            processStart();
          } else if (command.startsWith("connect ")) {
            String[] cmdLine = command.split(" ");
            processConnect(cmdLine[1], Integer.parseInt(cmdLine[2]),
                    cmdLine[3], Short.parseShort(cmdLine[4]));
          } else if (command.equals("neighbors")) {
            processNeighbors();
          } else if (command.equals("stats")) {
            processStats();
          } else if (command.equals("dump")) {
            System.out.print(lsd.toString());
          } else if (command.startsWith("send ")) {
            String[] cmdLine = command.split(" ", 3);
            if (cmdLine.length >= 3) {
              processSend(cmdLine[1], cmdLine[2]);
            } else {
              System.out.println("Usage: send [Destination IP] [Message]");
            }
          } else if (command.startsWith("update ")) {
            String[] cmdLine = command.split(" ");
            if (cmdLine.length >= 3) {
              processUpdate(Short.parseShort(cmdLine[1]), Short.parseShort(cmdLine[2]));
            } else {
              System.out.println("Usage: update [port_number] [new_weight]");
            }
          } else {
            System.out.println("Unknown command: " + command);
          }
        } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
          System.out.println("Invalid arguments. Check command usage.");
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
