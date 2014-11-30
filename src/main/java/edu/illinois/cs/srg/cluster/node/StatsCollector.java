package edu.illinois.cs.srg.cluster.node;


import edu.illinois.cs.srg.cluster.ClusterEmulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: In case of dynamic nodes, cpu , mem should be updated.
 * Created by gourav on 11/22/14.
 */
public class StatsCollector implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(StatsCollector.class);

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


      synchronized (node.resourceLock) {
        long time = (System.currentTimeMillis() - ClusterEmulator.startTime) / 1000;
        node.utilizations.put(time,
          new NodeUtilization(node.getCpuUsed(), node.getMemoryUsed()));
        //log.debug("{}, {}, {}", time, node.getCpuUsed(), node.getMemoryUsed());
      }
    }
  }
}
