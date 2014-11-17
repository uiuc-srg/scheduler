package edu.illinois.cs.srg.scheduler;

import java.io.Serializable;

/**
 * Created by gourav on 10/17/14.
 */
public class Task implements Serializable {

  long jobID;
  int index;
  double cpu;
  double memory;
  long duration;

  public long getJobID() {
    return jobID;
  }

  public int getIndex() {
    return index;
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

  /**
   * duration is in milli-seconds
   * @param jobID
   * @param index
   * @param cpu
   * @param memory
   * @param duration
   */
  public Task(long jobID, int index, double cpu, double memory, long duration) {
    this.jobID = jobID;
    this.index = index;
    this.cpu = cpu;
    this.memory = memory;
    this.duration = duration;
  }

}
