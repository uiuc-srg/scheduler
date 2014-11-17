package edu.illinois.cs.srg.serializables;

import edu.illinois.cs.srg.scheduler.Task;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by read on 10/24/14.
 * TODO: Combine multiple tasks into one placement requests
 */
public class PlacementRequest implements Serializable {
  Task task;

  public PlacementRequest(Task task) {
    this.task = task;
  }

  public Task getTask() {
    return task;
  }
}
