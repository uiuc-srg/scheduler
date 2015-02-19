package edu.illinois.cs.srg.scheduler.jobHandlers;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import edu.illinois.cs.srg.scheduler.*;
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
import java.util.HashMap;
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

  public abstract Map<Integer, Node> schedule(Map<Integer, TaskInfo> tasks);

  @Override
  public void run() {
    ScheduleRequest request = null;
    try {
      ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
      outputStream.flush();
      ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
      request = (ScheduleRequest) inputStream.readObject();
      this.jobID = request.getJobID();
      //log.info("Request: " + request);

      if (request.getJobID() == Constants.SIGTERM) {
        exit();
        return;
      }

      Map<Integer, TaskInfo> tasks = new HashMap<Integer, TaskInfo>(request.getTasks());
      ScheduleResponse response = schedule(tasks, request);
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


  public ScheduleResponse schedule(Map<Integer, TaskInfo> tasks, ScheduleRequest request) throws IOException {
    ScheduleResponse response = new ScheduleResponse(request.getJobID(), System.currentTimeMillis());
    Map<Integer, Long> sentTime = Maps.newHashMap();
    Map<Integer, Boolean> results = Maps.newHashMap();
    Map<Integer, Long> receiveTime = Maps.newHashMap();
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
        // A null value means NOT-GONNA-SCHEDULE-IT
        if (node == null) {
          sentTime.put(entry.getKey(), new Long(0));
          receiveTime.put(entry.getKey(), new Long(0));
          results.put(entry.getKey(), false);
          tries.put(entry.getKey(), attempts);
          tasks.remove(entry.getKey());
        } else {
          TaskInfo taskInfo = tasks.get(entry.getKey());
          sentTime.put(entry.getKey(), node.schedule(this, new PlacementRequest(request.getJobID(), entry.getKey(), taskInfo)));
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
          results.put(placementResponse.getIndex(), placementResponse.getResult());
          receiveTime.put(placementResponse.getIndex(), placementResponse.getReceiveTime());
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

    for (Map.Entry<Integer, TaskInfo> entry : tasks.entrySet()) {
      results.put(entry.getKey(), false);
      receiveTime.put(entry.getKey(), new Long(0));
      tries.put(entry.getKey(), attempts);
    }

    response.addResult(results, sentTime, receiveTime, result, tries);
    return response;
  }

  public void exit() throws IOException {
    try {
      Thread.sleep(2*Constants.TIMEOUT);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    PlacementRequest sigterm = Constants.createSIGTERMPlacementRequest();

    for (Node node : clusterState.getNodeList()) {
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
