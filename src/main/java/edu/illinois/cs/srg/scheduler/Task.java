package edu.illinois.cs.srg.scheduler;

import java.io.Serializable;

/**
 * Created by gourav on 10/17/14.
 */
public class Task implements Serializable {

  int index;
  double cpu;
  double memory;

  public Task(int index, double cpu, double memory) {
    this.index = index;
    this.cpu = cpu;
    this.memory = memory;
  }

}
