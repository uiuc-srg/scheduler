package edu.illinois.cs.srg.workload.yarn;

import com.google.common.collect.Maps;
import edu.illinois.cs.srg.scheduler.TaskInfo;
import edu.illinois.cs.srg.serializables.ScheduleRequest;
import edu.illinois.cs.srg.util.Constants;
import edu.illinois.cs.srg.workload.AbstractRequestGenerator;
import edu.illinois.cs.srg.workload.JobThread;
import edu.illinois.cs.srg.workload.google.GoogleTracePlayer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * Created by gourav on 3/9/15.
 */
public class YarnRequestGenerator implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(YarnRequestGenerator.class);

  GoogleTracePlayer player;
  YarnResponseServer responseServer;

  protected String name;
  protected String rmAddress;
  protected String myAddress;
  protected String mustangNM;
  protected String experiment;

  long experimentTime;
  long shutdownTime;

  public static Thread responseServerThread;

  public YarnRequestGenerator(String name, String rmAddress, String myAddress, String mustangNM, String experiment, long experimentTime, int speed, double suppressionFactor, double timeSuppressionFactor) throws IOException {
    this.name = name;
    this.rmAddress = rmAddress;
    this.myAddress = myAddress;
    this.mustangNM = mustangNM;
    this.experiment = experiment;

    this.experimentTime = experimentTime;
    //Setting shutdownTime to be 10 % of total time.
    this.shutdownTime = experimentTime / 10;

    player = new GoogleTracePlayer(name, experiment, experimentTime, speed, suppressionFactor, timeSuppressionFactor);

    responseServer = new YarnResponseServer(name, experiment);
    responseServerThread = new Thread(responseServer);
    responseServerThread.start();
  }

  @Override
  public void run() {
    long startTime = System.currentTimeMillis();
    long nJobs = 0;
    long nTasks = 0;
    long nTruncate = 0;
    while (responseServer.errors < AbstractRequestGenerator.MAX_ERROR) {
      ScheduleRequest request = player.getNextJob();
      if (request == null) {
        break;
      }
      nJobs++;

      if (request.getTasks().size() == 0) {
        log.warn("Job {} has zero tasks", request.getJobID());
        continue;
      }
      Map<Integer, Long> durations = Maps.newHashMap();
      int index = 0;
      double cpu = 0;
      double mem = 0;
      for (TaskInfo taskInfo : request.getTasks().values()) {
        long duration = taskInfo.getDuration();
        if (duration >  experimentTime + shutdownTime - (System.currentTimeMillis() - startTime)) {
          //We should truncate the duration.
          duration = experimentTime + shutdownTime - (System.currentTimeMillis() - startTime);
          nTruncate++;
        }

        nTasks++;
        durations.put(index, duration);
        cpu = taskInfo.getCpu();
        mem = taskInfo.getMemory();
        index++;

        if (duration + System.currentTimeMillis() > YarnExperiment.finishTime) {
          YarnExperiment.finishTime = duration + System.currentTimeMillis();
        }
      }

      Thread client = new Thread(new Client(rmAddress, myAddress, mustangNM, request.getJobID(), request.getTasks().size(), cpu, mem, durations));
      client.start();

    }
    log.info("YarnRequestGenerator: created {} jobs, {} tasks in {} seconds. Truncated durations for {} tasks.", nJobs, nTasks, (System.currentTimeMillis() - startTime)/1000, nTruncate);
    log.info("YarnRequestGenerator: Time to finish is less than {} seconds", (YarnExperiment.finishTime - System.currentTimeMillis())/1000);
    /*try {
      Thread.sleep(2* Constants.TIMEOUT);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }*/
  }

  public static void main(String[] args) {

  }

}
