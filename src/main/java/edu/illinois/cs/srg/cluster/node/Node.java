package edu.illinois.cs.srg.cluster.node;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edu.illinois.cs.srg.cluster.ClusterEmulator;
import edu.illinois.cs.srg.serializables.NodeInfo;
import edu.illinois.cs.srg.serializables.PlacementRequest;
import edu.illinois.cs.srg.serializables.PlacementResponse;
import edu.illinois.cs.srg.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Created by gourav on 9/3/14.
 * TODO: Should Node run as process or thread ?
 * TODO: Support for other resources.
 * TODO: Reserve resources for OS and scheduler programs.
 */
public class Node implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Node.class);

  private long id;

  // resources
  private double cpu;
  private double memory;
  private double memoryUsed;
  private double cpuUsed;

  // tasks
  Queue<Task> tasks;

  // connections
  Socket socket;
  ObjectInputStream input;
  ObjectOutputStream output;

  // threads
  Thread heart;
  Thread killer;
  Thread collector;

  // locks
  Object resourceLock;
  Object connectionLock;

  public Map<Long, NodeUtilization> utilizations;

  String logdir;

  private Node(long id, double cpu, double memory) {
    this.id = id;
    this.cpu = cpu;
    this.memory = memory;
    this.cpuUsed = 0;
    this.memoryUsed = 0;

    tasks = new PriorityQueue<Task>();
  }

  public Node(long id, double cpu, double memory, String schedulerAddress, int schedulerPort, String logdir) throws IOException{
    this(id, cpu, memory);

    this.socket = new Socket(schedulerAddress, schedulerPort);
    this.output = new ObjectOutputStream(socket.getOutputStream());
    this.output.flush();
    this.input = new ObjectInputStream(socket.getInputStream());


    this.output.writeObject(new NodeInfo(id, cpu, memory));
    this.output.flush();

    heart = new Thread(new Heart(this));
    killer = new Thread(new Killer(this));
    collector = new Thread(new StatsCollector(this));

    resourceLock = new Object();
    connectionLock = new Object();

    utilizations = Maps.newHashMap();

    this.logdir = logdir;
  }


  // TODO: Can fail without sending response.
  @Override
  public void run() {
    // start threads
    heart.start();
    killer.start();
    collector.start();

    try {
      while (true) {

        // wait for requests
        PlacementRequest request = (PlacementRequest) input.readObject();
        PlacementResponse response = new PlacementResponse(request.getJobID(), request.getIndex(), System.currentTimeMillis());
        //LOG.info("{} Received {}", this, request);

        if(request.getJobID() == Constants.SIGTERM) {
          exit();
          break;
        }

        // execute requests
        boolean result = false;
        synchronized (resourceLock) {
          if (add(request.getTaskInfo().getMemory(), request.getTaskInfo().getCpu())) {
            result = true;
            //LOG.debug("Added task {}", request);
            tasks.add(new Task(request.getJobID(), request.getIndex(), request.getTaskInfo()));
            resourceLock.notify();
          }
        }

        // write result / response
        response.addResult(result);
        synchronized (connectionLock) {
          output.writeObject(response);
          output.flush();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    try {
      Thread.sleep(2*edu.illinois.cs.srg.util.Constants.STATS_INTERVAL);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void exit() {
    // LOG.info("Shutting down {}", this);
    ClusterEmulator.terminate = true;
    PlacementResponse response = Constants.createSIGTERMPlacementResponse();
    synchronized (connectionLock) {
      try {
        output.writeObject(response);
        output.flush();

        output.close();
        input.close();
        socket.close();
      } catch (IOException e) {
        LOG.error("{}: error sending SIGTERM PlacementResponse", this);
        //e.printStackTrace();
      }
    }
    synchronized (resourceLock) {
      resourceLock.notifyAll();
    }

    try {
      Thread.sleep(Constants.HEARTBEAT_INTERVAL);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }

  public long getId() {
    return id;
  }

  public double getCpu() {
    return cpu;
  }

  public double getMemory() {
    return memory;
  }

  public double getAvailableCPU() {
    return cpu - cpuUsed;
  }

  public double getAvailableMemory() {
    return memory - memoryUsed;
  }

  public double getMemoryUsed() {
    return memoryUsed;
  }

  public double getCpuUsed() {
    return cpuUsed;
  }

  /**
   * Call after acquiring locks
   * Returns false if operation cannot be performed.
   * Returns true, otherwise.
   * @param memory
   * @param cpu
   * @return
   */
  private boolean add(double memory, double cpu) {
    if (memory < 0 || cpu < 0) {
      throw new RuntimeException("Memory and CPU should be non-negative: " + memory + ", " + cpu);
    }
    if (memory + memoryUsed <= this.memory && cpu + cpuUsed <= this.cpu) {
      memoryUsed += memory;
      cpuUsed += cpu;
      return true;
    }
    return false;
  }

  // call after acquiring locks
  private boolean check(double memory, double cpu) {
    if (memory < 0 || cpu < 0) {
      throw new RuntimeException("Memory and CPU should be non-negative: " + memory + ", " + cpu);
    }
    if (memory + memoryUsed <= this.memory && cpu + cpuUsed <= this.cpu) {
      return true;
    }
    return false;
  }


  // Should only be called after acquiring locks.
  public void release(double memory, double cpu) {
    //LOG.debug("{}: Resource are being released.", this);
    if (memory < 0 || cpu < 0) {
      throw new RuntimeException("Memory and CPU should be non-negative: " + memory + ", " + cpu);
    }
    memoryUsed = memoryUsed - memory;
    cpuUsed = cpuUsed - cpu;
  }

  @Override
  public String toString() {
    return new StringBuilder("Node[").append(id).append("]").toString();
  }

}
