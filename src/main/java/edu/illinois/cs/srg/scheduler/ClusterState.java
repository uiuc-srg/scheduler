package edu.illinois.cs.srg.scheduler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.*;

/**
 * Currently, contains only a set of nodes.
 * Need to provide locks too.
 *
 * Created by gourav on 11/15/14.
 */
public class ClusterState {
  Map<Long, Node> nodes;
  List<Long> ids;

  Object lock;
  Random random;

  public ClusterState() {
    this.nodes = Maps.newConcurrentMap();
    this.ids = Lists.newArrayList();
    lock = new Object();
    random = new Random();
  }

  public void add(Node node) {
    synchronized (lock) {
      nodes.put(node.getId(), node);
      ids.add(node.getId());
    }
  }

  public List<Long> getNodeIds() {
    return new ArrayList<Long>(ids);
  }

  public Node getRandom() {
    return nodes.get(ids.get(random.nextInt(ids.size())));
  }

  @Deprecated
  public Node getIndex(int index) {
    return nodes.get(ids.get(index));
  }

  public int size() {
    return nodes.size();
  }

  public Node get(long id) {
    return nodes.get(id);
  }

  public Collection<Node> getNodes() {
    return nodes.values();
  }
}
