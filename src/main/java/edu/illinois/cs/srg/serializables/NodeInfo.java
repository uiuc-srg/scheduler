package edu.illinois.cs.srg.serializables;

import java.io.Serializable;

/**
 * Created by gourav on 11/15/14.
 */
public class NodeInfo implements Serializable {
  long id;
  double cpu;
  double memory;

  public NodeInfo(long id, double cpu, double memory) {
    this.id = id;
    this.cpu = cpu;
    this.memory = memory;
  }

  public long getId() {
    return id;
  }

  public double getCpu() {
    return cpu;
  }

  public double getMemory() {
    return memory;
  }
}
