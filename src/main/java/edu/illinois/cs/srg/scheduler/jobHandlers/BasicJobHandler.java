package edu.illinois.cs.srg.scheduler.jobHandlers;

import com.google.common.collect.Maps;
import edu.illinois.cs.srg.scheduler.ClusterState;
import edu.illinois.cs.srg.scheduler.Node;
import edu.illinois.cs.srg.scheduler.TaskInfo;
import edu.illinois.cs.srg.scheduler.jobHandlers.AbstractJobHandler;

import java.net.Socket;
import java.util.Map;

/**
 * Created by gourav on 11/30/14.
 * Ten Try JobHandler
 */
public class BasicJobHandler extends AbstractJobHandler {

  public BasicJobHandler(ClusterState clusterState, Socket socket) {
    super(clusterState, socket);
  }

  @Override
  public Map<Integer, Node> schedule(Map<Integer, TaskInfo> tasks) {
    Map<Integer, Node> schedule = Maps.newHashMap();
    int maxTry = 10;

    for (Map.Entry<Integer, TaskInfo> entry : tasks.entrySet()) {
      TaskInfo taskInfo = entry.getValue();
      schedule.put(entry.getKey(), null);

      for (int i=0; i<maxTry; i++) {
        Node node = clusterState.getRandom();
        if (node.getAvailableCPU() >= taskInfo.cpu && node.getAvailableMemory() >= taskInfo.memory) {
          schedule.put(entry.getKey(), node);
          break;
        }
      }
    }
    return schedule;
  }
}
