package edu.illinois.cs.srg.serializables;

import edu.illinois.cs.srg.scheduler.TaskInfo;

import java.io.Serializable;

/**
 * Created by read on 10/24/14.
 * TODO: Combine multiple tasks into one placement requests
 */
public class PlacementRequest implements Serializable {
  long jobID;
  int index;
  TaskInfo taskInfo;

  public PlacementRequest(long jobID, int index, TaskInfo taskInfo) {
    this.jobID = jobID;
    this.index = index;
    this.taskInfo = taskInfo;
  }

  public int getIndex() {
    return index;
  }

  public long getJobID() {
    return jobID;
  }

  public TaskInfo getTaskInfo() {
    return taskInfo;
  }

  @Override
  public String toString() {
    return new StringBuilder("PlacementRequest[").append(jobID).append(", ").append(index).append("]").toString();
  }
}
