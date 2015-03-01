package edu.illinois.cs.srg.scheduler.monolithic;

import com.google.common.collect.Maps;
import edu.illinois.cs.srg.scheduler.ClusterState;
import edu.illinois.cs.srg.scheduler.Node;
import edu.illinois.cs.srg.scheduler.TaskInfo;
import edu.illinois.cs.srg.util.Constants;
import edu.illinois.cs.srg.workload.google.ConstraintInfo;

import java.net.Socket;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Created by gourav on 12/4/14.
 */
public class ConsJobHandler extends MonolithicJobHandler {

  public ConsJobHandler(ClusterState clusterState, Socket socket) {
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
        //log.info("Accessing index {}", index);
        Node node = clusterState.getIndex(index);
        if (node.getAvailableCPU() >= taskInfo.cpu && node.getAvailableMemory() >= taskInfo.memory && match(taskInfo.getCons(), node)) {
          schedule.put(entry.getKey(), node);
          break;
        }
      }
    }
    return schedule;
  }

  private boolean match(ConstraintInfo con, Node node) {
    if (con.getOperator() == Constants.CONS_EQUAL) {
      String supply = (node.getAttributes().containsKey(con.getName())) ? node.getAttributes().get(con.getName()) : "";
      String demand = con.getValue();
      return demand.equals(supply);
    } else if (con.getOperator() == Constants.CONS_NOT_EQUAL) {
      String supply = (node.getAttributes().containsKey(con.getName())) ? node.getAttributes().get(con.getName()) : "";
      String demand = con.getValue();
      return !demand.equals(supply);
    } else if (con.getOperator() == Constants.CONS_LESSER) {
      int supply = (node.getAttributes().containsKey(con.getName())) ? Integer.parseInt(node.getAttributes().get(con.getName())) : 0;
      int demand = Integer.parseInt(con.getValue());
      return (supply < demand);
    } else if (con.getOperator() == Constants.CONS_GREATER) {
      int supply = (node.getAttributes().containsKey(con.getName())) ? Integer.parseInt(node.getAttributes().get(con.getName())) : 0;
      int demand = Integer.parseInt(con.getValue());
      return (supply > demand);
    }
    return false;
  }

  private boolean match(Set<ConstraintInfo> cons, Node node) {
    for (ConstraintInfo con : cons) {
      if (!match(con, node)) {
        return false;
      }
    }
    return true;
  }
}
