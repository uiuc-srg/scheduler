package edu.illinois.cs.srg.scheduler;

import edu.illinois.cs.srg.serializables.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.LinkedList;
import java.util.Queue;

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

  // connections
  Socket socket;
  ObjectInputStream input;
  ObjectOutputStream output;

  Thread heartbeatServer;

  Queue<RequestInfo> pendingRequests;
  Object requestLock;


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

    this.heartbeatServer = new Thread(this);
    this.heartbeatServer.start();

    pendingRequests = new LinkedList<RequestInfo>();
    requestLock = new Object();

    LOG.info(this + " is created and started.");
  }

  public void update() throws IOException {
    try {
      Object object = input.readObject();
      try {
        Heartbeat heartbeat = (Heartbeat) object;
        //LOG.debug("{} for {}", heartbeat, this);
        this.availableCPU = heartbeat.availableCPU;
        this.availableMemory = heartbeat.availableMemory;
      } catch (ClassCastException e) {
        PlacementResponse placementResponse = (PlacementResponse) object;
        LOG.debug("{} for {}", placementResponse, this);
        // no lock required here
        RequestInfo requestInfo = pendingRequests.poll();
        if (requestInfo.request.getJobID() == placementResponse.getJobID() &&
          requestInfo.request.getIndex() == placementResponse.getIndex()) {
          requestInfo.jobHandler.addResponse(placementResponse);
          synchronized (requestInfo.jobHandler) {
            requestInfo.jobHandler.notify();
          }
        } else {
          LOG.error("{} is not compatible with {}", placementResponse, requestInfo.request);
        }
      }
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public String toString() {
    return new StringBuilder("Node[").append(id).append("]").toString();
  }

  @Override
  public void run() {
    try {
      while (!Scheduler.terminate) {
        update();
      }
      input.close();
      output.close();
    } catch(Exception e) {
      LOG.info("{} shutting down.", this);
      //e.printStackTrace();
    }
  }

  public long schedule(AbstractJobHandler jobHandler, PlacementRequest request) throws IOException {
    long sentTime = 0;
    synchronized (requestLock) {
      this.output.writeObject(request);
      this.output.flush();
      sentTime = System.currentTimeMillis();
      pendingRequests.add(new RequestInfo(jobHandler, request));
    }
    return sentTime;
  }


  static class RequestInfo {
    AbstractJobHandler jobHandler;
    PlacementRequest request;

    RequestInfo(AbstractJobHandler jobHandler, PlacementRequest request) {
      this.jobHandler = jobHandler;
      this.request = request;
    }
  }
}
