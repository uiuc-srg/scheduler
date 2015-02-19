package edu.illinois.cs.srg.scheduler.jobHandlers;

import com.google.common.collect.Maps;
import edu.illinois.cs.srg.scheduler.ClusterState;
import edu.illinois.cs.srg.scheduler.Node;
import edu.illinois.cs.srg.scheduler.TaskInfo;
import edu.illinois.cs.srg.scheduler.jobHandlers.AbstractJobHandler;

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
  public Map<Integer, Node> schedule(Map<Integer, TaskInfo> tasks) {
    Map<Integer, Node> schedule = Maps.newHashMap();
    for (Map.Entry<Integer, TaskInfo> task : tasks.entrySet()) {
      schedule.put(task.getKey(), clusterState.getRandom());
    }
    return schedule;
  }
}
