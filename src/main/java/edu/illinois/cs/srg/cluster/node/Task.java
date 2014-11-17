package edu.illinois.cs.srg.cluster.node;

/**
 * Created by gourav on 11/14/14.
 */
public class Task implements Comparable<Task> {
  long jobID;
  int index;
  double cpu;
  double memory;
  long endTimestamp;

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
  }

  public Task(edu.illinois.cs.srg.scheduler.Task task) {
    this.jobID = task.getJobID();
    this.index = task.getIndex();
    this.cpu = task.getCpu();
    this.memory = task.getMemory();
    this.endTimestamp = System.currentTimeMillis() + task.getDuration();
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
