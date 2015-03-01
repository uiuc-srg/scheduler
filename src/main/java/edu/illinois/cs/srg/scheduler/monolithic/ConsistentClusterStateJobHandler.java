package edu.illinois.cs.srg.scheduler.monolithic;

import com.google.common.collect.Maps;
import edu.illinois.cs.srg.scheduler.ClusterState;
import edu.illinois.cs.srg.scheduler.Node;
import edu.illinois.cs.srg.scheduler.TaskInfo;

import java.net.Socket;
import java.util.Map;

/**
 * Created by gourav on 2/13/15.
 */
public class ConsistentClusterStateJobHandler extends MonolithicJobHandler {

  public ConsistentClusterStateJobHandler(ClusterState clusterState, Socket socket) {
    super(clusterState, socket);
  }

  @Override
  public Map<Integer, Node> schedule(Map<Integer, TaskInfo> tasks) {
    //TODO: Use collections shuffling.
    Map<Integer, Node> schedule = Maps.newHashMap();
    int maxTry = 1000;

    for (Map.Entry<Integer, TaskInfo> entry : tasks.entrySet()) {
      TaskInfo taskInfo = entry.getValue();
      schedule.put(entry.getKey(), null);

      for (int i=0; i<maxTry; i++) {
        Node node = clusterState.getRandom();
        if (node.updateResource(taskInfo.cpu, taskInfo.memory)) {
          schedule.put(entry.getKey(), node);
          break;
        }
      }
    }
    return schedule;
  }
}
