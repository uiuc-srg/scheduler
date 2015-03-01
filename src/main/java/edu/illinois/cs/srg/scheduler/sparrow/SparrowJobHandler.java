package edu.illinois.cs.srg.scheduler.sparrow;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import edu.illinois.cs.srg.scheduler.AbstractJobHandler;
import edu.illinois.cs.srg.scheduler.ClusterState;
import edu.illinois.cs.srg.scheduler.TaskInfo;
import edu.illinois.cs.srg.serializables.monolithic.PlacementRequest;
import edu.illinois.cs.srg.serializables.PlacementResponse;
import edu.illinois.cs.srg.serializables.ScheduleRequest;
import edu.illinois.cs.srg.serializables.ScheduleResponse;
import edu.illinois.cs.srg.serializables.sparrow.SparrowRequest;
import edu.illinois.cs.srg.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by gourav on 2/22/15.
 */
public class SparrowJobHandler extends AbstractJobHandler {
  private static final Logger log = LoggerFactory.getLogger(SparrowJobHandler.class);

  boolean waiting = true;
  Map<Integer, TaskInfo> tasks;
  List<Integer> ids;
  long jobID;
  Set<PlacementResponse> placementResponses;


  public SparrowJobHandler(Socket socket, ClusterState clusterState) {
    super(socket, clusterState);
    placementResponses = Sets.newConcurrentHashSet();
    this.ids = Lists.newArrayList();
  }

  @Override
  public ScheduleResponse schedule(Map<Integer, TaskInfo> tasks, ScheduleRequest request, long receiveTime) throws IOException {
    this.tasks = tasks;
    this.ids.addAll(tasks.keySet());
    this.jobID = request.getJobID();
    // 1. send token to 2n random nodes.
    List<Long> nodes = clusterState.getNodeIds();
    Collections.shuffle(nodes);

    double maxCPU = 0;
    double maxMem = 0;
    for (Map.Entry<Integer, TaskInfo> entry : tasks.entrySet()) {
      maxMem = Math.max(maxMem, entry.getValue().memory);
      maxCPU = Math.max(maxCPU, entry.getValue().cpu);
    }
    SparrowRequest sparrowRequest = new SparrowRequest(request.getJobID(), maxCPU, maxMem);

    int candidates = Math.min(Constants.SPARROW_D*tasks.size(), nodes.size());
    if (candidates < Constants.SPARROW_D*tasks.size()) {
      log.warn("NOT enough nodes to probe for the job {}: needed {}, got {}", this, Constants.SPARROW_D, tasks.size());
    }
    for (int index=0; index<candidates; index++) {
      clusterState.get(nodes.get(index)).schedule(this, sparrowRequest);
    }

    // 2. Wait for response from first n nodes. Reply them with task descriptions.
    // No time-out
    // i don't think this is required

    synchronized (this) {
      try {
        this.wait();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    // TODO: Return responses combing all placement responses.
    return null;
  }

  @Override
  public void exit() throws IOException {

  }

  @Override
  public PlacementRequest addResponse(PlacementResponse response) {
    synchronized (this) {
      if (!waiting) {
        return null;
      }

      int index = ids.get(0);
      ids.remove(0);
      TaskInfo taskInfo = tasks.get(index);
      PlacementRequest request = new PlacementRequest(jobID, index, taskInfo);
      response.setIndex(index);
      response.setResult(true);
      placementResponses.add(response);

      if (ids.size() == 0) {
        waiting = false;
        this.notify();
      }
      return request;
    }
  }

  @Override
  public boolean shouldIKnock() {
    return waiting;
  }
}
