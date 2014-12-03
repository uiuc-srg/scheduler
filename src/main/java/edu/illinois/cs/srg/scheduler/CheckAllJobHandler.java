package edu.illinois.cs.srg.scheduler;

import com.google.common.collect.Maps;

import java.net.Socket;
import java.util.Map;
import java.util.Random;

/**
 * Created by gourav on 11/30/14.
 */
public class CheckAllJobHandler extends AbstractJobHandler {

  public CheckAllJobHandler(ClusterState clusterState, Socket socket) {
    super(clusterState, socket);
  }

  @Override
  public Map<Integer, Node> schedule(Map<Integer, TaskInfo> tasks) {
    Map<Integer, Node> schedule = Maps.newHashMap();
    Random random = new Random(System.currentTimeMillis());
    int size = clusterState.size();

    for (Map.Entry<Integer, TaskInfo> entry : tasks.entrySet()) {
      TaskInfo taskInfo = entry.getValue();
      schedule.put(entry.getKey(), null);

      int start = random.nextInt();
      for (int i=0; i<size; i++) {
        int index = (i+start) % size;
        Node node = clusterState.get(size);
        if (node.getAvailableCPU() >= taskInfo.cpu && node.getAvailableMemory() >= taskInfo.memory) {
          schedule.put(entry.getKey(), node);
          break;
        }
      }
    }
    return schedule;
  }
}
