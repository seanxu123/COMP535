# COMP535 Project Starter Code

## Requirements

To compile and run this project, you need:

* Java: Version 8 or higher (JDK 8+)
* Maven: Version 3.0 or higher

### Checking Your Versions

```
java -version
mvn -version
```

If you don't have these installed, please install them before proceeding.

## Building the Project

To compile the project:

```
mvn clean compile
```

To compile and create a JAR file:

```
mvn clean package
```

The compiled classes will be in `target/classes/` directory.

## Running the Program

Create an executable JAR with dependencies and run it:

```
mvn clean package assembly:single
java -jar target/COMP535-1.0-SNAPSHOT-jar-with-dependencies.jar conf/router1.conf
```

Run one instance per router, each in its own terminal, pointed at that router's own
`.conf` file. On startup, each router prints its Process IP, Process Port, and Simulated IP.
You'll need the Process IP/Port to `attach` to it from another terminal.

## Commands

```
attach [Process IP] [Process Port] [Simulated IP] [Link Weight]
start
connect [Process IP] [Process Port] [Simulated IP] [Link Weight]
update [Port Number] [New Weight]
disconnect [Port Number]
quit
neighbors
detect [Destination IP]
send [Destination IP] [Message]
stats
dump
```

### PA1

* `attach` opens a socket to the target router and requests a link; the receiving router is
  prompted `(Y/N)` to accept or reject. Does **not** trigger HELLO/state sync by itself.
* `start` begins the HELLO handshake **only on links belonging to the router it's run on**
  (not network-wide). To fully converge a multi-router topology, run `start` on every router
  (or at least one endpoint of every link).
* `neighbors` lists simulated IPs of routers currently in `TWO_WAY` status, which are attached
  **and** handshake-complete.

Each router supports up to 4 links and further `attach` attempts are rejected once
full, on both the initiating and accepting side.

### PA2

* `start` now also synchronizes the link state database. Whenever a link reaches `TWO_WAY`,
  the router rebuilds its own LSA (with an incremented sequence number) and floods its LSDB
  to its `TWO_WAY` neighbors in `LSAUPDATE` messages. A router that receives an LSAUPDATE only
  stores LSAs with a higher sequence number than the one it holds, and forwards only those
  newer LSAs to all neighbors except the one it received them from.
* `detect [Destination IP]` runs Dijkstra (using link weights, not hop count) over the LSDB and
  prints `Path found: <src> -> (cost) <hop> -> ... -> <dst>`, or `Path not found`.
* `send [Destination IP] [Message]` sends an application message hop by hop along the shortest
  path. Intermediate routers print the source, destination, and next hop but never the payload;
  only the destination prints the message.
* `stats` prints the number of HELLO, LSAUPDATE, APP_MESSAGE, WEIGHT_UPDATE, and LINK_DOWN messages
  sent and received since the router started.
* `dump` (debugging aid, not part of the spec) prints this router's current LSDB.

### PA3

* `connect [Process IP] [Process Port] [Simulated IP] [Link Weight]` works like `attach`, but can only
  be run after `start`. Once attached, it immediately kicks off the HELLO handshake with that neighbor,
  rebuilds the local LSA with an incremented sequence number, and floods `LSAUPDATE` to sync the network.
* `update [Port Number] [New Weight]` changes the link weight on that port (0 to 3). It sends a
  `WEIGHT_UPDATE` message (type 3) to the neighbor so both ends stay in sync, increments sequence
  numbers, and advertises the new LSAs.
* `disconnect [Port Number]` removes the link on that port, sends a `LINK_DOWN` message (type 4) to the
  neighbor, clears the port, increments the sequence number, and floods the updated LSA. The freed port
  can be reused later for `attach` or `connect`.
* `quit` shuts down the router. Before exiting, it clears all its ports, increments its sequence number,
  and sends a final empty LSA (`links = []`) followed by a `ROUTER_DOWN` message (type 5) to all active
  neighbors. Neighbors clean up their links and re-flood their own updated LSAs.

**Usage note:** flooding is asynchronous, so after running `start` or changing topology, give the
network a moment to converge (check with `dump`) before running `detect` or `send`.

## Troubleshooting

**"JAVA_HOME not set" error**

* Make sure Java is installed and JAVA_HOME environment variable points to your Java installation
* On Linux/Mac: `export JAVA_HOME=/path/to/java`
* On Windows: Set JAVA_HOME in System Environment Variables

**"Maven not found" error**

* Make sure Maven is installed and added to your PATH
* Verify with: `mvn -version`

**Compilation errors**

* Ensure you have Java 8 or higher installed
* Ensure you have Maven 3.0 or higher installed
* Try: `mvn clean compile` to rebuild from scratch

**"Could not find or load main class" error**

* Make sure you built the JAR file: `mvn clean package assembly:single`
* Verify the JAR exists: `ls target/COMP535-1.0-SNAPSHOT-jar-with-dependencies.jar`
* Rebuild the project: `mvn clean package assembly:single`
* Use the correct command: `java -jar target/COMP535-1.0-SNAPSHOT-jar-with-dependencies.jar conf/router1.conf`

**Maven settings.xml warning**

* If you see a warning about "Unrecognised tag: 'blocked'" in Maven settings.xml, you can safely ignore it
* This is a system-level Maven configuration issue and does not affect the build
* The build will still succeed despite this warning

## Project Structure

```
src/
  main/
    java/          # Source code
  resources/       # Configuration files
conf/              # Router configuration files
target/            # Compiled classes (generated)
```

## Implementation Notes

* Each `Link` keeps its socket connection open after `attach` or `connect`, and a background thread
  listens on it. HELLO, LSAUPDATE, APP_MESSAGE, WEIGHT_UPDATE, and failure notifications all travel
  over that same socket.
* Each router's LSA lists one entry per `TWO_WAY` neighbor. Dijkstra skips the weight-0 self-link.
* Link streams are reset (`ObjectOutputStream.reset()`) before every write so updated LSAs don't get
  replaced by cached copies.
* Restart the whole topology between test runs: LSA sequence numbers restart at 1 when a router
  process restarts, so neighbors would ignore a restarted router's new LSAs.

## Failure Detection

We use a combination of explicit protocol messages and socket-closure detection to catch when an
adjacency is lost:

* **Graceful disconnect / quit:** When `disconnect` runs, the router sends a `LINK_DOWN` packet
  (type 4) over the link before closing the socket. When `quit` runs, the quitting router floods its
  final empty LSA (`links = []`), sends a `ROUTER_DOWN` packet (type 5) to each neighbor, and closes its
  sockets before exiting.
* **Ungraceful failures:** If a router crashes or is killed unexpectedly (e.g. `kill -9`), the background
  thread listening on that link (`listenOnLink`) catches the `SocketException` or `EOFException` and
  triggers the same cleanup.
* **Duplicate notifications (idempotency):** Since an explicit notification is right before socket closure,
  the receiving router may catch both the packet and the closed socket. `handleLinkFailure` checks if
  the link is still in the `ports` array under `synchronized (ports)`. If it was already removed, it
  returns immediately. This ensures each adjacency loss only bumps the sequence number and floods
  an LSA once. Freed ports can immediately be reused for new `attach` or `connect` calls.

## Collaboration Statement

This project was completed collaboratively by **Hamza Chaudhry** and **Sean Xu**:

* **Hamza Chaudhry**: PA1 attach/start logic, PA2 LSDB sync and `stats`, PA3 `disconnect`, `quit`, and failure detection.
* **Sean Xu**: PA1 HELLO handshake, PA2 Dijkstra (`detect`) and message forwarding (`send`), PA3 `connect` and `update` weight.

