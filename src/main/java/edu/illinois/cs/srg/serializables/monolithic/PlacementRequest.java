package edu.illinois.cs.srg.serializables.monolithic;

import edu.illinois.cs.srg.scheduler.TaskInfo;
import edu.illinois.cs.srg.serializables.AbstractRequest;

import java.io.Serializable;

/**
 * Created by read on 10/24/14.
 * TODO: Combine multiple tasks into one placement requests
 */
public class PlacementRequest extends AbstractRequest {
  TaskInfo taskInfo;

  public PlacementRequest(long jobID, int index, TaskInfo taskInfo) {
    super(jobID, index);
    this.index = index;
    this.taskInfo = taskInfo;
  }

  public TaskInfo getTaskInfo() {
    return taskInfo;
  }

  @Override
  public String toString() {
    return new StringBuilder("PlacementRequest[").append(jobID).append(", ").append(index).append("]").toString();
  }
}
