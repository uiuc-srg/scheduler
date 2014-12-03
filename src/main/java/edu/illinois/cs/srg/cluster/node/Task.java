package edu.illinois.cs.srg.cluster.node;

import edu.illinois.cs.srg.scheduler.TaskInfo;

/**
 * Created by gourav on 11/14/14.
 */
public class Task implements Comparable<Task> {
  long jobID;
  int index;
  double cpu;
  double memory;
  long endTimestamp;
  long duration;
  long startTime;

  /**
   * @param jobID
   * @param index
   * @param cpu
   * @param memory
   * @param duration duration is in milli-seconds
   */
  public Task(long jobID, int index, double cpu, double memory, long duration) {
    this.jobID = jobID;
    this.index = index;
    this.cpu = cpu;
    this.memory = memory;
    this.endTimestamp = System.currentTimeMillis() + duration;
    this.duration = duration;
    this.startTime = System.currentTimeMillis();
  }

  public Task(long jobID, int index, TaskInfo taskInfo) {
    this.jobID = jobID;
    this.index = index;
    this.cpu = taskInfo.getCpu();
    this.memory = taskInfo.getMemory();
    this.endTimestamp = System.currentTimeMillis() + taskInfo.getDuration();
    this.duration = taskInfo.getDuration();
    this.startTime = System.currentTimeMillis();
  }

  @Override
  public int compareTo(Task other) {
    if (other.endTimestamp == this.endTimestamp) {
      return 0;
    } else if (other.endTimestamp >= this.endTimestamp) {
      return -1;
    } else {
      return 1;
    }
  }
}
