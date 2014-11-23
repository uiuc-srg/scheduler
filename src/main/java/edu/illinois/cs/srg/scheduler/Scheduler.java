package edu.illinois.cs.srg.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by gourav on 11/15/14.
 */
public class Scheduler implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(Scheduler.class);
  public static final boolean DEBUG = true;
  public static boolean terminate = false;

  ClusterState clusterState;

  // threads
  Thread nodeServer;
  JobServer jobServer;

  public Scheduler() {
    this.clusterState = new ClusterState();
    nodeServer = new Thread(new NodeServer(this.clusterState));
    jobServer = new JobServer(this.clusterState);
  }

  @Override
  public void run() {
    nodeServer.start();
    jobServer.run();
  }

  public static void main(String[] args) {
    log.info("Starting Scheduler.");
    Scheduler scheduler = new Scheduler();
    scheduler.run();
  }
}
