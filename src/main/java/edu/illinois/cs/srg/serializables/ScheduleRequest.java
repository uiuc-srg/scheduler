package edu.illinois.cs.srg.serializables;

import edu.illinois.cs.srg.scheduler.TaskInfo;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * Created by gourav on 10/17/14.
 */
public class ScheduleRequest implements Serializable {

  long jobID;
  Map<Integer, TaskInfo> tasks;

  public ScheduleRequest(long jobID, Map<Integer, TaskInfo> tasks) {
    this.jobID = jobID;
    this.tasks = tasks;
  }

  public long getJobID() {
    return jobID;
  }

  public Map<Integer, TaskInfo> getTasks() {
    return tasks;
  }

  @Override
  public String toString() {
    return new StringBuilder("ScheduleRequest[").append(jobID).append(",").append(tasks.size()).append("]").toString();
  }
}
