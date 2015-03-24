package edu.illinois.cs.srg.serializables;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import edu.illinois.cs.srg.scheduler.TaskInfo;
import edu.illinois.cs.srg.workload.google.ConstraintInfo;
import edu.illinois.cs.srg.workload.google.GoogleJob;

import java.io.Serializable;
import java.util.List;
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

  /**
   * Converting duration from micro-seconds to milli-seconds
   * @param googleJob
   */
  public ScheduleRequest(GoogleJob googleJob, int speed, double timeSuppressionFactor) {
    this.jobID = googleJob.getId();
    this.tasks = Maps.newHashMap();
    double cpu = googleJob.getCpu();
    double memory = googleJob.getMemory();

    int index = 0;
    List<Set<ConstraintInfo>> consFromJob = googleJob.getCons();
    for (long duration : googleJob.getDurations()) {
      // need to convert duration into milli-seconds.
      Set<ConstraintInfo> cons = Sets.newHashSet();
      if (index < consFromJob.size()) {
        cons = googleJob.getCons().get(index);
      }

      tasks.put(index, new TaskInfo(cpu, memory, (long) (duration / 1000 / speed / timeSuppressionFactor), cons));
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
    double cpu = (tasks == null || tasks.size() == 0) ? 0 : tasks.get(0).getCpu();
    double memory = (tasks == null || tasks.size() == 0) ? 0 : tasks.get(0).getMemory();
    return new StringBuilder("ScheduleRequest[").append(jobID).append(",").append(tasks.size()).append("," + cpu).append("," + memory).append("]").toString();
  }

}
