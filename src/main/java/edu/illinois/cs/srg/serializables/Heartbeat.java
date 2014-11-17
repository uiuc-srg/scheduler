package edu.illinois.cs.srg.serializables;

import java.io.Serializable;

/**
 * Created by gourav on 11/7/14.
 * TODO: Serializable is not network-efficient.
 *
 */
public class Heartbeat implements Serializable {

  public double availableCPU;
  public double availableMemory;

  public Heartbeat(double availableCPU, double availableMemory) {
    this.availableCPU = availableCPU;
    this.availableMemory = availableMemory;
  }

  @Override
  public String toString() {
    return new StringBuilder("Heartbeat[")
      .append(availableCPU).append(",")
      .append(availableMemory).append("]")
      .toString();
  }
}
