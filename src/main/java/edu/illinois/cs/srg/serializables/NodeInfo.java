package edu.illinois.cs.srg.serializables;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by gourav on 11/15/14.
 */
public class NodeInfo implements Serializable {
  long id;
  double cpu;
  double memory;

  Map<String, String> attributes;

  public NodeInfo(long id, double cpu, double memory, Map<String, String> attributes) {
    this.id = id;
    this.cpu = cpu;
    this.memory = memory;
    this.attributes = attributes;
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

  public Map<String, String> getAttributes() {
    return attributes;
  }

  @Override
  public String toString() {
    return new StringBuilder("NodeInfo[").append(id).append(", ")
      .append(cpu).append(", ")
      .append(memory).append("]")
      .toString();
  }
}
