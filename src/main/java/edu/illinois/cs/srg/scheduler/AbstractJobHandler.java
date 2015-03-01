package edu.illinois.cs.srg.scheduler;

import edu.illinois.cs.srg.serializables.PlacementResponse;
import edu.illinois.cs.srg.serializables.ScheduleRequest;
import edu.illinois.cs.srg.serializables.ScheduleResponse;
import edu.illinois.cs.srg.serializables.monolithic.PlacementRequest;
import edu.illinois.cs.srg.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by gourav on 2/22/15.
 */
public abstract class AbstractJobHandler implements Runnable {
  protected static final Logger log = LoggerFactory.getLogger(AbstractJobHandler.class);

  Socket socket;
  protected ClusterState clusterState;

  public AbstractJobHandler(Socket socket, ClusterState clusterState) {
    this.socket = socket;
    this.clusterState = clusterState;
  }

  public abstract ScheduleResponse schedule(Map<Integer, TaskInfo> tasks, ScheduleRequest request, long receiveTime) throws IOException;

  public abstract void exit() throws IOException;

  public abstract PlacementRequest addResponse(PlacementResponse response);

  public abstract boolean shouldIKnock();

  @Override
  public void run() {
    ScheduleRequest request = null;
    try {
      ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
      outputStream.flush();
      ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
      request = (ScheduleRequest) inputStream.readObject();
      long receiveTime = System.currentTimeMillis();
      //log.info("Request: " + request);

      if (request.getJobID() == Constants.SIGTERM) {
        exit();
        return;
      }

      Map<Integer, TaskInfo> tasks = new HashMap<Integer, TaskInfo>(request.getTasks());
      ScheduleResponse response = schedule(tasks, request, receiveTime);
      response.setSentSchedulerWG(System.currentTimeMillis());
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
}
