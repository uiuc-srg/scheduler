package edu.illinois.cs.srg.cluster;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import edu.illinois.cs.srg.cluster.node.MachineReader;
import edu.illinois.cs.srg.cluster.node.Node;
import edu.illinois.cs.srg.scheduler.Debugger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by gourav on 10/24/14.
 */
public class ClusterEmulator {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterEmulator.class);

  public static boolean terminate = false;

  String experiment;
  String schedulerAddress;
  int schedulerPort;
  Set<Thread> threads;
  Set<Node> nodes;

  Thread monitor;

  String logdir;

  public static long startTime;

  public static BufferedWriter kills;
  public static Object killLock;

  public ClusterEmulator(String experiment, String schedulerAddress, int schedulerPort) throws IOException {
    this.experiment = experiment;
    this.schedulerAddress = schedulerAddress;
    this.schedulerPort = schedulerPort;
    this.threads = Sets.newHashSet();
    this.nodes = Sets.newHashSet();

    logdir = System.getProperty("user.home") + "/logs/" + experiment;

    File dir = new File(logdir);
    dir.mkdirs();

    monitor = new Thread(new ClusterMonitor(System.getProperty("user.home") + "/logs/" + experiment + "/monitor"));

    kills = new BufferedWriter(new FileWriter(new File(logdir + "/kills")));
    killLock = new Object();
  }

  public void homogeneousCluster(int numberNodes) {
    for (int i=0; i<numberNodes; i++) {
      try {
        Node node = new Node(i, 0.5, 0.2493, new HashMap<String, String>(), schedulerAddress, schedulerPort, logdir);
        nodes.add(node);
        Thread thread = new Thread(node);
        thread.start();
        threads.add(thread);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public void heterogeneousCluster(int limit) throws IOException {
    MachineReader reader = new MachineReader();
    Set<MachineReader.MachineInfo> machines = reader.getMachines(limit);

   for (MachineReader.MachineInfo machineInfo : machines) {
      try {
        Node node = new Node(machineInfo.id, machineInfo.cpu, machineInfo.memory, machineInfo.attributes, schedulerAddress, schedulerPort, logdir);
        nodes.add(node);
        Thread thread = new Thread(node);
        thread.start();
        threads.add(thread);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }


  // Initialize and start up Node Threads
  public void initialize(int numberNodes) throws IOException {
    monitor.start();



    startTime = System.currentTimeMillis();
    homogeneousCluster(numberNodes);
    //heterogeneousCluster(numberNodes);

    LOG.debug("All nodes started.");
    for (Thread node : threads) {
      try {
        node.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    long totalSeconds = (System.currentTimeMillis() - startTime) / 1000;

    try {
      kills.close();
      BufferedWriter writer = new BufferedWriter(new FileWriter(new File(System.getProperty("user.home") + "/logs/" + experiment + "/utilization")));
      double totalCpu = 0;
      double totalMemory = 0;
      Map<Long, Double> lastCpuUsage = Maps.newHashMap();
      Map<Long, Double> lastMemoryUsage = Maps.newHashMap();
      for (Node node : nodes) {
        totalCpu += node.getCpu();
        totalMemory += node.getMemory();
        lastCpuUsage.put(node.getId(), 0.0);
        lastMemoryUsage.put(node.getId(), 0.0);
      }

      //DecimalFormat formatter = new DecimalFormat("##.##");
      long offset = -1;
      for (long time = 0; time <= totalSeconds; time++) {
        double usedCpu = 0;
        double usedMemory = 0;
        for (Node node : nodes) {
          if (node.utilizations.containsKey(time)) {
            //LOG.debug("Found usage for time {}", time);
            lastCpuUsage.put(node.getId(), node.utilizations.get(time).getCpuUsage());
            lastMemoryUsage.put(node.getId(), node.utilizations.get(time).getMemoryUsage());
          }
          usedCpu += lastCpuUsage.get(node.getId());
          usedMemory += lastMemoryUsage.get(node.getId());
        }
        if ((usedCpu > 0 || usedMemory > 0) && offset == -1) {
          offset = time;
        }
        double cpuUtil = usedCpu / totalCpu;
        double memoryUtil = usedMemory / totalMemory;
        if (offset != -1) {
          writer.write((time - offset) + ", " + cpuUtil + ", " + memoryUtil);
          writer.newLine();
        }
      }
      writer.flush();
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    LOG.info("All nodes shut down. ClusterEmulator is shutting down.");
  }

  public static void main(String[] args) {
    LOG.info("Starting ClusterEmulator.");
    String schedulerAddress = "127.0.0.1";
    if (args.length > 1) {
      schedulerAddress = args[1];
    }

    try {
      ClusterEmulator emulator = new ClusterEmulator(args[0], schedulerAddress, edu.illinois.cs.srg.util.Constants.NODE_SERVER_PORT);
      emulator.initialize(Integer.parseInt(args[2]));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
