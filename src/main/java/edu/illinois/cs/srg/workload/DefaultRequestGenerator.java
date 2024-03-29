package edu.illinois.cs.srg.workload;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import edu.illinois.cs.srg.scheduler.TaskInfo;
import edu.illinois.cs.srg.serializables.ScheduleRequest;
import edu.illinois.cs.srg.workload.google.ConstraintInfo;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

/**
 * Created by gourav on 11/22/14.
 */
public class DefaultRequestGenerator extends AbstractRequestGenerator {

  int requestCount = 0;

  protected DefaultRequestGenerator(String name, String schedulerAddress, String experiment) throws IOException {
    super(name, schedulerAddress, experiment);
  }

  @Override
  public ScheduleRequest getNextRequest() {
    if (requestCount++ > 10) {
      return null;
    }
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Map<Integer, TaskInfo> tasks = Maps.newHashMap();
    tasks.put(0, new TaskInfo(0.25, 0.25, 50000, new HashSet<ConstraintInfo>()));
    tasks.put(1, new TaskInfo(0.25, 0.25, 50000, new HashSet<ConstraintInfo>()));
    return new ScheduleRequest(requestCount, tasks);
  }
}
