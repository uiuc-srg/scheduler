package edu.illinois.cs.srg.workload.yarn;

import edu.illinois.cs.srg.util.Constants;
import edu.illinois.cs.srg.workload.WGMonitor;
import edu.illinois.cs.srg.workload.WorkloadGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Created by gourav on 3/22/15.
 */
public class YarnExperiment {
  private static final Logger log = LoggerFactory.getLogger(YarnExperiment.class);

  public static boolean terminate = false;
  public static long finishTime = 0;

  String rmAddress;
  String myAddress;
  String mustangNM;
  String experiment;
  Thread monitor;

  long duration;
  int speed;
  double suppressionFactor;
  double timeSuppressionFactor;

  public YarnExperiment(String experiment, String rmAddress, String myAddress, String mustangNM) {
    this.experiment = experiment;
    this.rmAddress = rmAddress;
    this.myAddress = myAddress;
    this.mustangNM = mustangNM;
    duration = 4*60*60*1000;
    speed = 100;
    suppressionFactor = 1;
    timeSuppressionFactor = 1;
  }

  public YarnExperiment(String experiment, String rmAddress, String myAddress, String mustangNM, long duration, int speed, double suppressionFactor, double timeSuppressionFactor) {
    this.experiment = experiment;
    this.rmAddress = rmAddress;
    this.myAddress = myAddress;
    this.mustangNM = mustangNM;
    this.duration = duration;
    this.speed = speed;
    this.suppressionFactor = suppressionFactor;
    this.timeSuppressionFactor = timeSuppressionFactor;

  }

  public void generate() {
    log.info("Duration {} seconds, Speed {}x, Suppression {}x, timeSuppression {}x", duration / 1000, speed, suppressionFactor, timeSuppressionFactor);

    try {
      String logdir = System.getProperty("user.home") + "/logs/" + experiment;
      File dir = new File(logdir);
      dir.mkdirs();

      monitor = new Thread(new WGMonitor(logdir + "/monitor"));
      monitor.start();

      Thread requestGenerator = new Thread(new YarnRequestGenerator("google", rmAddress, myAddress, mustangNM, experiment, duration, speed, suppressionFactor, timeSuppressionFactor));
      requestGenerator.start();
      requestGenerator.join();

    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    exit();
  }

  public void exit() {
    // Make sure we have received all responses. Wait maximum for 10 minutes (10*speed minutes).
    while (YarnResponseServer.waitingJobs.size() > 0 &&
      YarnExperiment.finishTime > System.currentTimeMillis()) {
      try {
        Thread.sleep((YarnExperiment.finishTime - System.currentTimeMillis()) / 10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (IllegalArgumentException e) {
        e.printStackTrace();
      }
    }

    long startTime = System.currentTimeMillis();
    while (YarnResponseServer.waitingJobs.size() > 0 &&
      (System.currentTimeMillis() - startTime) < 100*Constants.WORKLOAD_SIGTERM_WAIT) {
      try {
        Thread.sleep(Constants.WORKLOAD_SIGTERM_WAIT / 10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    if (YarnResponseServer.waitingJobs.size() > 0) {
      log.warn("There are still {} jobs waiting.", YarnResponseServer.waitingJobs.size());
    }

    YarnRequestGenerator.responseServerThread.interrupt();
    try {
      Thread.sleep(Constants.WORKLOAD_SIGTERM_WAIT);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    try {
      YarnResponseServer.jobWriter.close();
      YarnResponseServer.taskWriter.close();
      log.warn("Shutting up Yarn Experiment");
    } catch (IOException e) {
      e.printStackTrace();
    }


    WorkloadGenerator.terminate = true;
  }


  public static void main(String args[])  {
    log.info("Starting WorkloadGenerator for Yarn Experiment.");
    String experiment = args[0];
    String rmAddress = args[1];
    String myAddress = args[2];
    double suppression = 1.0;
    if (args.length > 5) {
      suppression = Double.parseDouble(args[5]) / Double.parseDouble(args[6]);
    }
    String mustangNM = args[7];
    YarnExperiment yarnExperiment = new YarnExperiment(experiment, rmAddress, myAddress, mustangNM, Integer.parseInt(args[3])*1000, Integer.parseInt(args[4]), suppression, Double.parseDouble(args[8]));
    yarnExperiment.generate();
  }
}
