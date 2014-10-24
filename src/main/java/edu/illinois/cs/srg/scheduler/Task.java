package edu.illinois.cs.srg.scheduler;

import java.io.Serializable;

/**
 * Created by gourav on 10/17/14.
 */
public class Task implements Serializable {
    public double getDuration() {
        return duration;
    }

    int index;

  public int getIndex() {
    return index;
  }

  public double getCpu() {
    return cpu;
  }

  public double getMemory() {
    return memory;
  }

  double cpu;
  double memory;
  double duration;

  //TODO: add TASK Length to the task
  public Task(int index, double cpu, double memory) {
    this.index = index;
    this.cpu = cpu;
    this.memory = memory;
  }

}
