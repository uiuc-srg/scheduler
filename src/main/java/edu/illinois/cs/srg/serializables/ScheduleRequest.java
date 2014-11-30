package edu.illinois.cs.srg.serializables;

import com.google.common.collect.Maps;
import edu.illinois.cs.srg.scheduler.TaskInfo;
import edu.illinois.cs.srg.workload.google.GoogleJob;

import java.io.Serializable;
import java.util.Map;

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

  /**
   * Converting duration from micro-seconds to milli-seconds
   * @param googleJob
   */
  public ScheduleRequest(GoogleJob googleJob, int speed) {
    this.jobID = googleJob.getId();
    this.tasks = Maps.newHashMap();
    double cpu = googleJob.getCpu();
    double memory = googleJob.getMemory();

    int index = 0;
    for (long duration : googleJob.getDurations()) {
      tasks.put(index, new TaskInfo(cpu, memory, duration / 1000 / speed));
      index++;
    }
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
