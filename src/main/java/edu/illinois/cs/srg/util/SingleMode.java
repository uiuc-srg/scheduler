package edu.illinois.cs.srg.util;

import edu.illinois.cs.srg.cluster.ClusterEmulator;
import edu.illinois.cs.srg.scheduler.Scheduler;
import edu.illinois.cs.srg.workload.WorkloadGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by gourav on 2/12/15.
 */
public class SingleMode {

  private static final Logger log = LoggerFactory.getLogger(SingleMode.class);
  static String experiment;

  public static void main(String[] args) {
    experiment = "test";

    // 1. Start scheduler.
    log.info("Starting Scheduler.");
    Scheduler.experiment = experiment;
    Thread scheduler = null;
    try {
      scheduler = new Thread(new Scheduler());
      scheduler.start();
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // 2. Start Cluster TODO: user/home/traces/machines
    log.info("Starting ClusterEmulator.");
    String schedulerAddress = "127.0.0.1";
    try {
      Thread emulator = new Thread(new ClusterRunnable(new ClusterEmulator(experiment, schedulerAddress, edu.illinois.cs.srg.util.Constants.NODE_SERVER_PORT)));
      emulator.start();
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // 3. Start Workload Generator
    log.info("Starting WorkloadGenerator.");
    WorkloadGenerator generator = new WorkloadGenerator(experiment, schedulerAddress);
    generator.generate();

  }

  static class ClusterRunnable implements Runnable {

    ClusterEmulator clusterEmulator;

    public ClusterRunnable(ClusterEmulator clusterEmulator) {
      this.clusterEmulator = clusterEmulator;
    }

    @Override
    public void run() {
      try {
        clusterEmulator.initialize(100);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
