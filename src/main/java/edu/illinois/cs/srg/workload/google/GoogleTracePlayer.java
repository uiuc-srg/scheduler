package edu.illinois.cs.srg.workload.google;

import edu.illinois.cs.srg.serializables.ScheduleRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Created by gourav on 3/21/15.
 */
public class GoogleTracePlayer {
  private static final Logger log = LoggerFactory.getLogger(GoogleTracePlayer.class);

  List<GoogleJob> googleJobs;

  // micro-seconds
  long offset;

  // milli-seconds
  long startTime;

  int next;

  BufferedWriter timestampWriter;

  long experimentTime;

  int speed;

  double timeSuppressionFactor;

  public GoogleTracePlayer(String name, String experiment, long experimentTime, int speed, double suppressionFactor, double timeSuppressionFactor) throws IOException {
    String tracedir = System.getProperty("user.home") + "/traces/";
    TraceReader reader = new TraceReader(tracedir + "/attributes", tracedir + "/durationsNoNaN", tracedir +"/constraints", suppressionFactor);
    googleJobs = reader.getJobs();

    next = Integer.MAX_VALUE;
    offset = -1;
    startTime = -1;

    for (int i=0; i< googleJobs.size(); i++) {
      if (googleJobs.get(i).timestamp > 0) {
        offset = googleJobs.get(i).timestamp;
        next = i;
        break;
      }
    }

    String logdir = System.getProperty("user.home") + "/logs/" + experiment + "/";
    timestampWriter = new BufferedWriter(new FileWriter(new File(logdir + "/" + name + ".timestamps")));

    this.experimentTime = experimentTime;

    this.speed = speed;
    this.timeSuppressionFactor = timeSuppressionFactor;
  }

  public ScheduleRequest getNextJob() {

    if (next >= googleJobs.size() || (startTime != -1 && (System.currentTimeMillis() - startTime) > experimentTime)) {
      try {
        timestampWriter.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }

    if (startTime == -1) {
      startTime = System.currentTimeMillis();
    } else {
      long interArrivalInterval = (googleJobs.get(next).timestamp - offset) / 1000 / speed;
      try {
        Thread.sleep(interArrivalInterval - (System.currentTimeMillis() - startTime ));
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (IllegalArgumentException e) {
        // ignore
      }
      try {
        timestampWriter.write((System.currentTimeMillis() - startTime) + ", " + interArrivalInterval );
        timestampWriter.newLine();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    ScheduleRequest request = new ScheduleRequest(googleJobs.get(next), speed, timeSuppressionFactor);
    next++;
    return request;
  }

  public static void main(String[] args) {
    try {
      GoogleTracePlayer player = new GoogleTracePlayer("google", "test", 100000, 10, 1.0, 1.0);
      while (true) {
        ScheduleRequest request = player.getNextJob();
        System.out.println(request);

        if (request == null) {
          break;
        }

      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
