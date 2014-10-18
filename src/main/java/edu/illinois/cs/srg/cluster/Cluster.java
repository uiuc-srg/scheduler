package edu.illinois.cs.srg.cluster;

import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by gourav on 10/17/14.
 */
public class Cluster {

  public static Map<Long, Node> nodes;
  public static Map<Long, Usage> usage;

  public static Object lock;

  public static void init() {
    lock = new Object();
    nodes = new HashMap<Long, Node>();
    usage = Maps.newHashMap();
    // create 12K nodes with 0.5 cpu and memory.
    for (long i=0; i<12000; i++) {
      nodes.put(i, new Node(i, 0.5, 0.5));
      usage.put(i, new Usage());
    }
  }

}
