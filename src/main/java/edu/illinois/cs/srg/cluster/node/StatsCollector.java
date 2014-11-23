package edu.illinois.cs.srg.cluster.node;


import edu.illinois.cs.srg.cluster.ClusterEmulator;

/**
 * TODO: In case of dynamic nodes, cpu , mem should be updated.
 * Created by gourav on 11/22/14.
 */
public class StatsCollector implements Runnable {

  Node node;
  double cpu;
  double memory;


  public StatsCollector(Node node) {
    this.node = node;
    this.cpu = node.getCpu();
    this.memory = node.getMemory();
  }

  @Override
  public void run() {
    while (!ClusterEmulator.terminate) {

      //sleep
      try {
        Thread.sleep(edu.illinois.cs.srg.util.Constants.STATS_INTERVAL);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      node.utilizations.add(new NodeUtilization(System.currentTimeMillis(),
        (cpu - node.getAvailableCPU()) / cpu,
        (memory - node.getAvailableMemory()) / memory));
    }
  }
}
