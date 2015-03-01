package edu.illinois.cs.srg.scheduler.monolithic;

import com.google.common.collect.Maps;
import edu.illinois.cs.srg.scheduler.ClusterState;
import edu.illinois.cs.srg.scheduler.Node;
import edu.illinois.cs.srg.scheduler.TaskInfo;

import java.net.Socket;
import java.util.Map;
import java.util.Random;

/**
 * Created by gourav on 11/30/14.
 */
public class CheckAllJobHandler extends MonolithicJobHandler {

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

      int start = random.nextInt(size);
      for (int i=0; i<size; i++) {
        int index = (i+start) % size;
        Node node = clusterState.getIndex(index);
        if (node.getAvailableCPU() >= taskInfo.cpu && node.getAvailableMemory() >= taskInfo.memory) {
          schedule.put(entry.getKey(), node);
          break;
        }
      }
    }
    return schedule;
  }
}
