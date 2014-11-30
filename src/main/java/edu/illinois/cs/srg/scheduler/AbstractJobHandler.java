package edu.illinois.cs.srg.scheduler;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import edu.illinois.cs.srg.serializables.PlacementRequest;
import edu.illinois.cs.srg.serializables.PlacementResponse;
import edu.illinois.cs.srg.serializables.ScheduleRequest;
import edu.illinois.cs.srg.serializables.ScheduleResponse;
import edu.illinois.cs.srg.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Map;
import java.util.Set;

/**
 * Created by read on 10/24/14.
 */
public abstract class AbstractJobHandler implements Runnable {
  protected static final Logger log = LoggerFactory.getLogger(AbstractJobHandler.class);

  Socket socket;
  Set<PlacementResponse> placementResponses;
  ClusterState clusterState;
  boolean waiting = true;

  long jobID;

  public AbstractJobHandler(ClusterState clusterState, Socket socket) {
    this.socket = socket;
    placementResponses = Sets.newConcurrentHashSet();
    this.clusterState = clusterState;
    this.jobID = 0;
  }

  public abstract Map<Integer, Node> schedule(ScheduleRequest scheduleRequest);

  @Override
  public void run() {
    ScheduleRequest request = null;
    try {
      ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
      outputStream.flush();
      ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
      request = (ScheduleRequest) inputStream.readObject();
      this.jobID = request.getJobID();
      ScheduleResponse response = new ScheduleResponse(request.getJobID(), System.currentTimeMillis());
      //log.info("Request: " + request);

      if (request.getJobID() == Constants.SIGTERM) {
        exit();
        return;
      }

      Map<Integer, Node> schedule = schedule(request);

      if (schedule.size() != request.getTasks().size()) {
        log.error("{}: schedule size does not match request size");
      }

      Map<Integer, Long> sentTime = Maps.newHashMap();
      for (Map.Entry<Integer, Node> entry : schedule.entrySet()) {
        Node node = entry.getValue();
        TaskInfo taskInfo = request.getTasks().get(entry.getKey());
        sentTime.put(entry.getKey(), node.schedule(this, new PlacementRequest(request.getJobID(), entry.getKey(), taskInfo)));
      }

      //log.debug("{}: waiting for results.", this);
      int result = ScheduleResponse.SUCCESS;
      long startTime = System.currentTimeMillis();
      try {
        while (placementResponses.size() < schedule.size() &&
          Constants.TIMEOUT - (System.currentTimeMillis() - startTime) > 0) {
          try {
            synchronized (this) {
              this.wait(Constants.TIMEOUT - (System.currentTimeMillis() - startTime));
            }
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      } catch (IllegalArgumentException e) {
        e.printStackTrace();
      }
      if (placementResponses.size() < schedule.size()) {
        result = ScheduleResponse.FAIL;
        log.error("{}: timed-out while waiting for response.", this);
      } else {
        //log.debug("{}: got all results", this);
      }
      waiting = false;

      // create n write response
      Map<Integer, Boolean> results = Maps.newHashMap();
      Map<Integer, Long> receiveTime = Maps.newHashMap();
      for (PlacementResponse placementResponse : placementResponses) {
        results.put(placementResponse.getIndex(), placementResponse.getResult());
        receiveTime.put(placementResponse.getIndex(), placementResponse.getReceiveTime());
      }

      response.addResult(results, sentTime, receiveTime, result);
      //log.info("Response: " + response);
      outputStream.writeObject(response);
      outputStream.flush();
      //log.debug("{}: wrote the result", this);
      inputStream.close();
      outputStream.close();
      socket.close();
    } catch (IOException e) {
      log.error("Discarding request {}", request);
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public void exit() throws IOException {
    try {
      Thread.sleep(2*Constants.TIMEOUT);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    PlacementRequest sigterm = Constants.createSIGTERMPlacementRequest();

    for (Node node : clusterState.nodeList) {
      node.schedule(this, sigterm);
    }

    Scheduler.terminate = true;

    log.info("Shutting down Scheduler.");
  }

  public void addResponse(PlacementResponse response) {
    placementResponses.add(response);
  }

  public boolean shouldIKnock() {
    return waiting;
  }

  @Override
  public String toString() {
    return "JobHandler[" + jobID + "]";
  }
}
