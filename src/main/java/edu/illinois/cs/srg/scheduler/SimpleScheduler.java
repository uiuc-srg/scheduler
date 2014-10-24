package edu.illinois.cs.srg.scheduler;

import edu.illinois.cs.srg.cluster.SimpleClusterState;
import edu.illinois.cs.srg.cluster.Node;
import edu.illinois.cs.srg.cluster.Usage;
import edu.illinois.cs.srg.interfaces.cluster.ClusterState;
import edu.illinois.cs.srg.interfaces.scheduler.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Created by gourav on 10/17/14.
 */
public class SimpleScheduler extends Scheduler {

    public SimpleScheduler(Socket socket) {
        super(socket);
    }

    @Override
    public ScheduleResponse schedule(ScheduleRequest scheduleRequest) {
        ClusterState clusterState = new SimpleClusterState();
        //NOTE: This should be called by the scheduler when making scheduler decisions
        // Place the tasks in the cluster
        int numTasksScheduled = 0;
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
        return new ScheduleResponse(success);
    }
}
