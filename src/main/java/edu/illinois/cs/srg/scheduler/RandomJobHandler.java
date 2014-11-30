package edu.illinois.cs.srg.scheduler;

import com.google.common.collect.Maps;
import edu.illinois.cs.srg.serializables.ScheduleRequest;

import java.net.Socket;
import java.util.Map;

/**
 * Created by gourav on 10/17/14.
 */
public class RandomJobHandler extends AbstractJobHandler {

  public RandomJobHandler(ClusterState clusterState, Socket socket) {
    super(clusterState, socket);
  }

  @Override
  public Map<Integer, Node> schedule(ScheduleRequest scheduleRequest) {
    Map<Integer, Node> schedule = Maps.newHashMap();
    for (Map.Entry<Integer, TaskInfo> task : scheduleRequest.getTasks().entrySet()) {
      schedule.put(task.getKey(), clusterState.getRandom());
    }
    return schedule;
  }
}
