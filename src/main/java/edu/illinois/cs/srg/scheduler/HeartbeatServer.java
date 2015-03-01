package edu.illinois.cs.srg.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by gourav on 11/15/14.
 */
public class HeartbeatServer implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(HeartbeatServer.class);

  ClusterState clusterState;

  public HeartbeatServer(ClusterState clusterState) {
    this.clusterState = clusterState;
  }

  @Override
  public void run() {
    // this will keep receiving heartbeat for EVERY node.
    /*while (true) {
      Iterator<Node> iterator = clusterState.getIterator();
      while (iterator.hasNext()) {
        try {
          iterator.next().update();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if (Scheduler.DEBUG) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }*/
  }

  @Override
  public String toString() {
    return "HeartbeatServer";
  }
}
