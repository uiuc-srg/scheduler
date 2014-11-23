package edu.illinois.cs.srg.cluster;

import com.google.common.collect.Sets;
import edu.illinois.cs.srg.cluster.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;

/**
 * Created by read on 10/24/14.
 */
public class ClusterEmulator {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterEmulator.class);

  public static boolean terminate = false;

  String experiment;
  String schedulerAddress;
  int schedulerPort;
  Set<Thread> nodes;

  public ClusterEmulator(String experiment, String schedulerAddress, int schedulerPort) {
    this.experiment = experiment;
    this.schedulerAddress = schedulerAddress;
    this.schedulerPort = schedulerPort;
    this.nodes = Sets.newHashSet();
  }

  // Initialize and start up Node Threads
  // TODO: Read node resources from a file
  public void initialize(int numberNodes) {

    String logdir = "~/logs/" + experiment + "/cluster";
    File dir = new File(logdir);
    dir.mkdirs();

    for (int i=0; i<numberNodes; i++) {
      try {
        Thread node = new Thread(new Node(i, 0.5, 0.5, schedulerAddress, schedulerPort, logdir));
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
    LOG.info("Starting ClusterEmulator.");
    String schedulerAddress = "127.0.0.1";
    if (args.length > 1) {
      schedulerAddress = args[1];
    }
    ClusterEmulator emulator = new ClusterEmulator(args[0], schedulerAddress, edu.illinois.cs.srg.util.Constants.NODE_SERVER_PORT);
    emulator.initialize(2);
  }
}
