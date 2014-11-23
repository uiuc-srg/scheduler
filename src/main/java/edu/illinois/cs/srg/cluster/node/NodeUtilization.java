package edu.illinois.cs.srg.cluster.node;

/**
 * Created by gourav on 11/22/14.
 */
public class NodeUtilization {
  long timestamp;
  double cpuUtilization;
  double memoryUtilization;

  public NodeUtilization(long timestamp, double cpuUtilization, double memoryUtilization) {
    this.timestamp = timestamp;
    this.cpuUtilization = cpuUtilization;
    this.memoryUtilization = memoryUtilization;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public double getCpuUtilization() {
    return cpuUtilization;
  }

  public double getMemoryUtilization() {
    return memoryUtilization;
  }

  @Override
  public String toString() {
    return timestamp + ", " + cpuUtilization + ", " + memoryUtilization;
  }
}
