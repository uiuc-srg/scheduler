package edu.illinois.cs.srg.cluster;

import com.google.common.collect.Maps;
import edu.illinois.cs.srg.scheduler.ScheduleRequest;
import edu.illinois.cs.srg.scheduler.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by gourav on 10/17/14.
 */
public class Cluster {

  private static final Logger log = LoggerFactory.getLogger(Cluster.class);
  private static final int DEFAULT_CLUSTER_SIZE = 12000;
  public static Map<Long, Node> nodes;
  public static Map<Long, Usage> usage;


  public static Object lock;

  //TODO: Make it to where we can specify the number & kind of machines
  public static void init() {
    lock = new Object();
    nodes = new HashMap<Long, Node>();
    usage = Maps.newHashMap();
    // create 12K nodes with 0.5 cpu and memory.
    for (long i = 0; i < DEFAULT_CLUSTER_SIZE; i++) {
      nodes.put(i, new Node(i, 0.5, 0.5));
      usage.put(i, new Usage());
    }
  }

  public Cluster() {
    this(DEFAULT_CLUSTER_SIZE);
  }

  //TODO: Should this also take a ClusterDescription ? This could be a set of node descriptions to specify kinds of nodes
  public Cluster(int numMachines) {
    lock = new Object();
    nodes = new HashMap<Long, Node>();
    usage = Maps.newHashMap();

    // create 12K nodes with 0.5 cpu and memory.
    for (long i = 0; i < numMachines; i++) {
      nodes.put(i, new Node(i, 0.5, 0.5));
      usage.put(i, new Usage());
    }
  }

  public Map<Long, Usage> getUsage() {
    return usage;
  }

  //NOTE: This should be called by the scheduler when making scheduler decisions
  public boolean schedule(ScheduleRequest scheduleRequest) {
    // Place the tasks in the cluster
    int numTasksScheduled = 0;
    synchronized (this.lock) {
      for (Task task : scheduleRequest.getTasks()) {
        // Random first fit.
        long nodeID = -1;
        for (Node node : Cluster.nodes.values()) {
          Usage usage = Cluster.usage.get(node.getId());
          if ((node.getCpu() - usage.cpu >= task.getCpu()) && (node.getMemory() - usage.memory >= task.getMemory())) {
            nodeID = node.getId();
            usage.cpu += task.getCpu();
            usage.memory += task.getMemory();
            numTasksScheduled++;
            log.info("Task {}-{} got scheduled on {}", scheduleRequest.getId(), task.getIndex(), nodeID);
            break;
          }
        }
      }
    }
    return (numTasksScheduled == scheduleRequest.getTasks().size());
  }

  //NOTE: Called by remote cluster to update/plsce jobs
  public boolean updateUsage(Map<Long, Usage> usage) {
    //TODO: Add logic here
    return false;
  }
}
