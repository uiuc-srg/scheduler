package edu.illinois.cs.srg.serializables;

import edu.illinois.cs.srg.scheduler.Task;

import java.io.Serializable;
import java.util.Set;

/**
 * Created by gourav on 10/17/14.
 */
public class ScheduleRequest implements Serializable {

  public long getId() {
    return id;
  }

  // Job id
  long id;
  Set<Task> tasks;

  public Set<Task> getTasks() {
    return tasks;
  }

  public ScheduleRequest(long id, Set<Task> tasks) {
    this.id = id;
    this.tasks = tasks;
  }

}
