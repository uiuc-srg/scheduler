package edu.illinois.cs.srg.scheduler;

import edu.illinois.cs.srg.scheduler.jobHandlers.AbstractJobHandler;
import edu.illinois.cs.srg.serializables.*;
import edu.illinois.cs.srg.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by gourav on 11/15/14.
 */
public class Node implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Node.class);

  private long id;

  // resources
  private double cpu;
  private double memory;
  private double availableCPU;
  private double availableMemory;
  private Object resourceLock;

  // connections
  Socket socket;
  ObjectInputStream input;
  ObjectOutputStream output;

  Thread node;

  Queue<RequestInfo> pendingRequests;
  Object requestLock;

  Map<String, String> attributes;

  public Node(Socket socket) throws IOException, ClassNotFoundException {
    this.socket = socket;
    this.output = new ObjectOutputStream(socket.getOutputStream());
    this.output.flush();
    this.input = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));

    // Ask for id, cpu, mem
    NodeInfo nodeInfo = (NodeInfo) input.readObject();
    this.id = nodeInfo.getId();
    this.cpu = nodeInfo.getCpu();
    this.memory = nodeInfo.getMemory();
    this.availableCPU = this.cpu;
    this.availableMemory = this.memory;
    this.resourceLock = new Object();

    this.attributes = nodeInfo.getAttributes();

    this.node = new Thread(this);
    this.node.start();

    pendingRequests = new ConcurrentLinkedQueue<RequestInfo>();
    requestLock = new Object();

    Debugger.increment();
    //LOG.info(this + " is created and started.");
  }

  /**
   * Returns if we should terminate.
   * @return
   * @throws IOException
   */
  public boolean update() throws IOException {
    try {
      Object object = input.readObject();
      try {
        Heartbeat heartbeat = (Heartbeat) object;
        //LOG.debug("{} for {}", heartbeat, this);
        synchronized (resourceLock) {
          this.availableCPU = heartbeat.availableCPU;
          this.availableMemory = heartbeat.availableMemory;
        }
      } catch (ClassCastException e) {
        PlacementResponse placementResponse = (PlacementResponse) object;
        placementResponse.setRecvSchedulerCluster(System.currentTimeMillis());
        //LOG.debug("{} for {}", placementResponse, this);

        if (placementResponse.getJobID() == Constants.SIGTERM) {
          //LOG.debug("{}: Got SIGTERM", this);
          if (pendingRequests.size() > 0) {
            LOG.warn("{} shutting down with some requests pending.", this);
          }
          return true;
        }

        // no lock required here

        boolean success = false;

        while (pendingRequests.size() > 0) {
          RequestInfo requestInfo = pendingRequests.poll();
          placementResponse.setSentSchedulerCluster(requestInfo.sentSchedulerCluster);
          if (requestInfo.request.getJobID() == placementResponse.getJobID() &&
            requestInfo.request.getIndex() == placementResponse.getIndex()) {

            synchronized (requestInfo.jobHandler) {
              if (requestInfo.jobHandler.shouldIKnock()) {
                requestInfo.jobHandler.addResponse(placementResponse);
                requestInfo.jobHandler.notify();
              } else {
                LOG.error("{}: JobHandler do not want the response no more", this);
              }
            }
            success = true;
            break;
          } else {
            // request should have matched.
            LOG.error("ERROR 2");
          }
        }

        if (!success) {
          // there should have been a request.
          LOG.error("ERROR 1", placementResponse);
        }
      }
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (NullPointerException e) {
      e.printStackTrace();
    }
    return false;
  }

  public String toString() {
    return new StringBuilder("Node[").append(id).append("]").toString();
  }

  @Override
  public void run() {
    while (true) {
      try {
        if (update()) {
          break;
        }
      } catch (IOException e) {
        LOG.info("{} got {}.", this, e);
        e.printStackTrace();
      }
    }
    try {
      input.close();
      output.close();
      socket.close();
    } catch (IOException e1) {
      //e1.printStackTrace();
    }
    //LOG.info("{} shutting down.", this);
    Debugger.decrement();
  }

  public void schedule(AbstractJobHandler jobHandler, PlacementRequest request) throws IOException {
    synchronized (requestLock) {
      if (request.getJobID() != Constants.SIGTERM) {
        pendingRequests.add(new RequestInfo(jobHandler, request, System.currentTimeMillis()));
      }
      this.output.writeObject(request);
      this.output.flush();
    }

  }


  static class RequestInfo {
    AbstractJobHandler jobHandler;
    PlacementRequest request;
    long sentSchedulerCluster;

    RequestInfo(AbstractJobHandler jobHandler, PlacementRequest request, long sentSchedulerCluster) {
      this.jobHandler = jobHandler;
      this.request = request;
      this.sentSchedulerCluster = sentSchedulerCluster;
    }
  }

  public double getAvailableCPU() {
    return availableCPU;
  }

  public double getAvailableMemory() {
    return availableMemory;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public boolean updateResource(double cpuRequest, double memRequest) {
    synchronized (resourceLock) {
      if (cpuRequest <= availableCPU && memRequest <= availableMemory) {
        availableCPU -= cpuRequest;
        availableMemory -= memRequest;
        return true;
      }
      return false;
    }
  }
}
