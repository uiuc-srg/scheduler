package edu.illinois.cs.srg.scheduler;

/**
 * Created by gourav on 11/15/14.
 */
public class Scheduler implements Runnable {
  public static final boolean DEBUG = true;

  ClusterState clusterState;

  // threads
  Thread nodeServer;
  JobServer jobServer;

  public Scheduler() {
    this.clusterState = new ClusterState();
    nodeServer = new Thread(new NodeServer(this.clusterState));
    jobServer = new JobServer();
  }

  @Override
  public void run() {
    nodeServer.start();
    jobServer.run();
  }

  public static void main(String[] args) {
    Scheduler scheduler = new Scheduler();
    scheduler.run();
  }
}
