package edu.illinois.cs.srg.cluster;

import com.google.common.collect.Sets;
import edu.illinois.cs.srg.cluster.node.Constants;
import edu.illinois.cs.srg.cluster.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

/**
 * Created by read on 10/24/14.
 */
public class ClusterEmulator {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterEmulator.class);

  String schedulerAddress;
  int schedulerPort;
  Set<Thread> nodes;

  public ClusterEmulator(String schedulerAddress, int schedulerPort) {
    this.schedulerAddress = schedulerAddress;
    this.schedulerPort = schedulerPort;
    this.nodes = Sets.newHashSet();
  }

  // Initialize and start up Node Threads
  // TODO: Read node resources from a file
  public void initialize(int numberNodes) {
    for (int i=0; i<numberNodes; i++) {
      try {
        Thread node = new Thread(new Node(i, 0.5, 0.5, schedulerAddress, schedulerPort));
        node.start();
        nodes.add(node);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    LOG.debug("All nodes started.");
    for (Thread node : nodes) {
      try {
        node.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) {
    ClusterEmulator emulator = new ClusterEmulator("127.0.0.1", Constants.NODE_SERVER_PORT);
    emulator.initialize(2);
  }
}
