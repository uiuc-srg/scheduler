package edu.illinois.cs.srg.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Created by gourav on 11/15/14.
 */
public class Scheduler implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(Scheduler.class);
  public static final boolean DEBUG = true;
  public static boolean terminate = false;

  public static String experiment;

  ClusterState clusterState;

  // threads
  Thread nodeServer;
  JobServer jobServer;
  Thread monitor;

  public Scheduler() throws IOException {
    String logdir = System.getProperty("user.home") + "/logs/" + experiment;
    File dir = new File(logdir);
    dir.mkdirs();

    this.clusterState = new ClusterState();
    nodeServer = new Thread(new NodeServer(this.clusterState));
    jobServer = new JobServer(this.clusterState);
    monitor = new Thread(new SchedulerMonitor(System.getProperty("user.home") + "/logs/" + experiment + "/monitor"));
  }

  @Override
  public void run() {
    nodeServer.start();
    monitor.start();
    jobServer.run();
  }

  public static void main(String[] args) {
    log.info("Starting Scheduler.");
    experiment = args[0];
    Scheduler scheduler = null;
    try {
      scheduler = new Scheduler();
      scheduler.run();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
