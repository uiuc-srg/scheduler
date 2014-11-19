package edu.illinois.cs.srg.scheduler;

import java.io.Serializable;

/**
 * Created by gourav on 10/17/14.
 */
public class TaskInfo implements Serializable {

  double cpu;
  double memory;
  long duration;

  /**
   * duration is in milli-seconds
   * @param cpu
   * @param memory
   * @param duration
   */
  public TaskInfo( double cpu, double memory, long duration) {
    this.cpu = cpu;
    this.memory = memory;
    this.duration = duration;
  }

  public double getCpu() {
    return cpu;
  }

  public double getMemory() {
    return memory;
  }

  public long getDuration() {
    return duration;
  }
}
