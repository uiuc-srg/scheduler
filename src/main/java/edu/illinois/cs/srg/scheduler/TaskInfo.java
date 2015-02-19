package edu.illinois.cs.srg.scheduler;

import edu.illinois.cs.srg.workload.google.ConstraintInfo;

import java.io.Serializable;
import java.util.Set;

/**
 * Created by gourav on 10/17/14.
 */
public class TaskInfo implements Serializable {

  public double cpu;
  public double memory;
  public long duration;

  Set<ConstraintInfo> cons;

  /**
   * duration is in milli-seconds
   * @param cpu
   * @param memory
   * @param duration
   */
  public TaskInfo( double cpu, double memory, long duration, Set<ConstraintInfo> cons) {
    this.cpu = cpu;
    this.memory = memory;
    this.duration = duration;
    this.cons = cons;
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

  public Set<ConstraintInfo> getCons() {
    return cons;
  }
}
