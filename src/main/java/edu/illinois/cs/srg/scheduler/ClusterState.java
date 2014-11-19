package edu.illinois.cs.srg.scheduler;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.*;

/**
 * Currently, contains only a set of nodes.
 * Need to provide locks too.
 * Created by gourav on 11/15/14.
 */
public class ClusterState {
  Set<Node> nodes;
  List<Node> nodeList;

  Object lock;
  Random random;

  public ClusterState() {
    this.nodes = Sets.newConcurrentHashSet();
    this.nodeList = Lists.newArrayList();
    lock = new Object();
    random = new Random();
  }

  public void add(Node node) {
    synchronized (lock) {
      nodes.add(node);
      nodeList.add(node);
    }
  }

  public Node getRandom() {
    return nodeList.get(random.nextInt(nodeList.size()));
  }

  @Deprecated
  // Need to provide concurrency control
  public Iterator<Node> getIterator() {
    return nodes.iterator();
  }
}
