package edu.illinois.cs.srg.scheduler;

import edu.illinois.cs.srg.serializables.ScheduleRequest;
import edu.illinois.cs.srg.serializables.ScheduleResponse;

import java.net.Socket;

/**
 * Created by gourav on 10/17/14.
 */
public class DefaultJobHandler extends AbstractJobHandler {

  public DefaultJobHandler(Socket socket) {
    super(socket);
  }

  @Override
  public ScheduleResponse schedule(ScheduleRequest scheduleRequest) {
        /*int numTasksScheduled = 0;
        synchronized (clusterState.getLock()) {
            for (Task task : scheduleRequest.getTasks()) {
                // Random first fit.
                long nodeID = -1;
                for (Node node : SimpleClusterState.nodes.values()) {
                    Usage usage = SimpleClusterState.usage.get(node.getId());
                    if ((node.getCpu() - usage.cpu >= task.getCpu()) && (node.getMemory() - usage.memory >= task.getMemory())) {
                        nodeID = node.getId();
                        usage.cpu += task.getCpu();
                        usage.memory += task.getMemory();
                        numTasksScheduled++;
                        log.info("Task {}-{} got scheduled on {}", scheduleRequest.getId(), task.getIndex(), nodeID);
                        break;
                    }
                }
            }
        }
        boolean success = (numTasksScheduled == scheduleRequest.getTasks().size());
        return new ScheduleResponse(success);*/
    return new ScheduleResponse(false);
  }
}
