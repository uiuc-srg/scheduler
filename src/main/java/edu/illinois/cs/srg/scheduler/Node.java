package edu.illinois.cs.srg.scheduler;

import edu.illinois.cs.srg.serializables.Heartbeat;
import edu.illinois.cs.srg.serializables.NodeInfo;
import edu.illinois.cs.srg.serializables.PlacementResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

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

    LOG.info(this + " is created and started.");
  }

  public void update() throws IOException {
    try {
      try {
        Heartbeat heartbeat = (Heartbeat) input.readObject();
        LOG.debug("{} for {}", heartbeat, this);
        this.availableCPU = heartbeat.availableCPU;
        this.availableMemory = heartbeat.availableMemory;
      } catch (ClassCastException e) {
        PlacementResponse placementResponse = (PlacementResponse) input.readObject();
        LOG.debug("{} for {}", placementResponse, this);
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
      while (true) {
        update();
        if (Scheduler.DEBUG) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    } catch(Exception e) {
      e.printStackTrace();
    }
  }
}
