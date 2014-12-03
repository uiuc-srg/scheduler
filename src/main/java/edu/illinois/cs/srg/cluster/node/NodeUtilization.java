package edu.illinois.cs.srg.cluster.node;

/**
 * Created by gourav on 11/22/14.
 */
public class NodeUtilization {
  double cpuUsage;
  double memoryUsage;

  public NodeUtilization(double cpuUsage, double memoryUsage) {
    this.cpuUsage = cpuUsage;
    this.memoryUsage = memoryUsage;
  }

  public double getCpuUsage() {
    return cpuUsage;
  }

  public double getMemoryUsage() {
    return memoryUsage;
  }

  @Override
  public String toString() {
    return cpuUsage + ", " + memoryUsage;
  }
}
