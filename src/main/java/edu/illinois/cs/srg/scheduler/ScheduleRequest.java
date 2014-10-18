package edu.illinois.cs.srg.scheduler;

import java.io.Serializable;
import java.util.Set;

/**
 * Created by gourav on 10/17/14.
 */
public class ScheduleRequest implements Serializable {

  // Job id
  long id;
  Set<Task> tasks;

  public ScheduleRequest(long id, Set<Task> tasks) {
    this.id = id;
    this.tasks = tasks;
  }

}
