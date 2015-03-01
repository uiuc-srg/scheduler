package edu.illinois.cs.srg.scheduler.monolithic;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import edu.illinois.cs.srg.scheduler.*;
import edu.illinois.cs.srg.serializables.monolithic.PlacementRequest;
import edu.illinois.cs.srg.serializables.PlacementResponse;
import edu.illinois.cs.srg.serializables.ScheduleRequest;
import edu.illinois.cs.srg.serializables.ScheduleResponse;
import edu.illinois.cs.srg.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.Set;

/**
 * Created by read on 10/24/14.
 */
public abstract class MonolithicJobHandler extends AbstractJobHandler {
  private static final Logger log = LoggerFactory.getLogger(MonolithicJobHandler.class);

  Set<PlacementResponse> placementResponses;
  boolean waiting = true;
  long jobID;

  public MonolithicJobHandler(ClusterState clusterState, Socket socket) {
    super(socket, clusterState);
    placementResponses = Sets.newConcurrentHashSet();
    this.jobID = 0;
  }

  public abstract Map<Integer, Node> schedule(Map<Integer, TaskInfo> tasks);

  public ScheduleResponse schedule(Map<Integer, TaskInfo> tasks, ScheduleRequest request, long receiveTime) throws IOException {
    this.jobID = request.getJobID();
    ScheduleResponse response = new ScheduleResponse(request.getJobID(), receiveTime);
    Map<Integer, Long> sentSchedulerClusterTimes = Maps.newHashMap();
    Map<Integer, Long> recvClusterTimes = Maps.newHashMap();
    Map<Integer, Long> sentClusterTimes = Maps.newHashMap();
    Map<Integer, Long> recvSchedulerClusterTimes = Maps.newHashMap();
    Map<Integer, Boolean> results = Maps.newHashMap();
    Map<Integer, Integer> tries = Maps.newHashMap();
    int attempts = 0;
    int result = ScheduleResponse.SUCCESS;
    boolean outstandingRequestsPending = false;

    while(tasks.size() > 0 && ++attempts <= Constants.MAX_ATTEMPTS && !outstandingRequestsPending) {
      placementResponses = Sets.newConcurrentHashSet();
      waiting = true;

      Map<Integer, Node> schedule = schedule(tasks);
      if (schedule.size() != tasks.size()) {
        log.error("{}: schedule size does not match request size");
      }

      for (Map.Entry<Integer, Node> entry : schedule.entrySet()) {
        Node node = entry.getValue();
        // A null value means NOT-GONNA-SCHEDULE-IT GONNA-FAIL
        if (node == null) {
          sentSchedulerClusterTimes.put(entry.getKey(), new Long(0));
          recvClusterTimes.put(entry.getKey(), new Long(0));
          sentClusterTimes.put(entry.getKey(), new Long(0));
          recvSchedulerClusterTimes.put(entry.getKey(), new Long(0));
          results.put(entry.getKey(), false);
          tries.put(entry.getKey(), attempts);
          tasks.remove(entry.getKey());
          result = ScheduleResponse.FAIL;
        } else {
          TaskInfo taskInfo = tasks.get(entry.getKey());
          node.schedule(this, new PlacementRequest(request.getJobID(), entry.getKey(), taskInfo));
        }
      }

      //log.debug("{}: waiting for results.", this);

      long startTime = System.currentTimeMillis();
      synchronized (this) {
        while (placementResponses.size() < tasks.size() &&
          Constants.TIMEOUT - (System.currentTimeMillis() - startTime) > 0) {
          try {
            this.wait(Constants.TIMEOUT - (System.currentTimeMillis() - startTime));
            //this.wait();
          } catch (InterruptedException e) {
            e.printStackTrace();
          } catch (IllegalArgumentException e) {
            e.printStackTrace();
          }
        }
        waiting = false;
      }

      if (placementResponses.size() < tasks.size()) {
        log.error("{}: timed-out while waiting for response.", this);
        // ERROR State - the code is not suitable to go for another round with outstanding requests pending.
        // Therefore, we need to exit with a few failed requests.
        outstandingRequestsPending = true;
        SchedulerMonitor.incrementTimeouts();
      } else {
        //log.debug("{}: got all results", this);
      }

      // create n write response
      for (PlacementResponse placementResponse : placementResponses) {
        if (placementResponse.getResult()) {
          sentSchedulerClusterTimes.put(placementResponse.getIndex(), placementResponse.getSentSchedulerCluster());
          recvClusterTimes.put(placementResponse.getIndex(), placementResponse.getRecvCluster());
          sentClusterTimes.put(placementResponse.getIndex(), placementResponse.getSentCluster());
          recvSchedulerClusterTimes.put(placementResponse.getIndex(), placementResponse.getRecvSchedulerCluster());

          results.put(placementResponse.getIndex(), placementResponse.getResult());
          tries.put(placementResponse.getIndex(), attempts);
          tasks.remove(placementResponse.getIndex());
        } else {
          //log.error("Node replied false for {}", placementResponse);
          SchedulerMonitor.incrementAttempts();
        }
      }
    }

    if (attempts >= Constants.MAX_ATTEMPTS) {
      log.error("Maximum attempts reached for job {}: {}", request, attempts);
    }

    if (tasks.size() > 0) {
      result = ScheduleResponse.FAIL;
    }

    // GONNA-FAIL
    for (Map.Entry<Integer, TaskInfo> entry : tasks.entrySet()) {
      sentSchedulerClusterTimes.put(entry.getKey(), new Long(0));
      recvClusterTimes.put(entry.getKey(), new Long(0));
      sentClusterTimes.put(entry.getKey(), new Long(0));
      recvSchedulerClusterTimes.put(entry.getKey(), new Long(0));

      results.put(entry.getKey(), false);
      tries.put(entry.getKey(), attempts);
    }
    response.addResult(
      sentSchedulerClusterTimes, recvClusterTimes,
      sentClusterTimes, recvSchedulerClusterTimes,
      results, tries, result);
    return response;
  }

  public void exit() throws IOException {
    try {
      Thread.sleep(2*Constants.TIMEOUT);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    PlacementRequest sigterm = Constants.createSIGTERMPlacementRequest();

    for (Node node : clusterState.getNodes()) {
      node.schedule(this, sigterm);
    }

    Scheduler.terminate = true;

    log.info("Shutting down Scheduler.");
  }

  @Override
  public PlacementRequest addResponse(PlacementResponse response) {
    placementResponses.add(response);
    return null;
  }

  public boolean shouldIKnock() {
    return waiting;
  }

  @Override
  public String toString() {
    return "JobHandler[" + jobID + "]";
  }
}
