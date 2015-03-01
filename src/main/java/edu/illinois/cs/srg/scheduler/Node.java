package edu.illinois.cs.srg.scheduler;

import edu.illinois.cs.srg.serializables.*;
import edu.illinois.cs.srg.serializables.monolithic.PlacementRequest;
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
public abstract class Node implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Node.class);

  protected long id;

  // resources
  protected double cpu;
  protected double memory;
  protected double availableCPU;
  protected double availableMemory;
  protected Object resourceLock;

  // connections
  protected Socket socket;
  protected ObjectInputStream input;
  protected ObjectOutputStream output;

  Thread node;

  protected Queue<RequestInfo> pendingRequests;
  protected Object requestLock;

  protected Map<String, String> attributes;

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
  public abstract boolean update() throws IOException;

  public abstract void schedule(AbstractJobHandler jobHandler, AbstractRequest request) throws IOException;

  protected static class RequestInfo {
    public AbstractJobHandler jobHandler;
    public AbstractRequest request;
    public long sentSchedulerCluster;

    public RequestInfo(AbstractJobHandler jobHandler, AbstractRequest request, long sentSchedulerCluster) {
      this.jobHandler = jobHandler;
      this.request = request;
      this.sentSchedulerCluster = sentSchedulerCluster;
    }
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

  public long getId() {
    return id;
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

  public String toString() {
    return new StringBuilder("Node[").append(id).append("]").toString();
  }
}
