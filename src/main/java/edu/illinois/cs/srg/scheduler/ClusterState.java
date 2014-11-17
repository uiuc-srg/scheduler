package edu.illinois.cs.srg.scheduler;

import com.google.common.collect.Sets;

import java.util.Iterator;
import java.util.Set;

/**
 * Currently, contains only a set of nodes.
 * Need to provide locks too.
 * Created by gourav on 11/15/14.
 */
public class ClusterState {
  Set<Node> nodes;

  public ClusterState() {
    this.nodes = Sets.newConcurrentHashSet();
  }

  public void add(Node node) {
    nodes.add(node);
  }

  public Iterator<Node> getIterator() {
    return nodes.iterator();
  }
}
