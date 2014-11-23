package edu.illinois.cs.srg.scheduler;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sun.tools.internal.jxc.apt.Const;
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

  public AbstractJobHandler(ClusterState clusterState, Socket socket) {
    this.socket = socket;
    placementResponses = Sets.newHashSet();
    this.clusterState = clusterState;
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
      ScheduleResponse response = new ScheduleResponse(request.getJobID(), System.currentTimeMillis());
      log.info("Received: " + request);

      if (request.getJobID() == Constants.SIGTERM) {
        exit();
        return;
      }

      Map<Integer, Node> schedule = schedule(request);
      Map<Integer, Long> sentTime = Maps.newHashMap();

      for (Map.Entry<Integer, Node> entry : schedule.entrySet()) {
        Node node = entry.getValue();
        TaskInfo taskInfo = request.getTasks().get(entry.getKey());
        sentTime.put(entry.getKey(), node.schedule(this, new PlacementRequest(request.getJobID(), entry.getKey(), taskInfo)));
      }

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
        log.error("{} timed-out while waiting for response.");
      }

      // create n write response
      Map<Integer, Boolean> results = Maps.newHashMap();
      Map<Integer, Long> receiveTime = Maps.newHashMap();
      for (PlacementResponse placementResponse : placementResponses) {
        results.put(placementResponse.getIndex(), placementResponse.getResult());
        receiveTime.put(placementResponse.getIndex(), placementResponse.getReceiveTime());
      }

      response.addResult(results, sentTime, receiveTime, result);
      outputStream.writeObject(response);


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
    PlacementRequest sigterm = Constants.createSIGTERMPlacementRequest();

    for (Node node : clusterState.nodeList) {
      node.schedule(this, sigterm);
    }

    try {
      Thread.sleep(Constants.SCHEDULER_SIGTERM_WAIT);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Scheduler.terminate = true;
  }

  public void addResponse(PlacementResponse response) {
    placementResponses.add(response);
  }
}
