package edu.illinois.cs.srg.workload.google;

import edu.illinois.cs.srg.serializables.ScheduleRequest;
import edu.illinois.cs.srg.workload.AbstractRequestGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Created by gourav on 11/25/14.
 */
public class GoogleRequestGenerator extends AbstractRequestGenerator {
  private static final Logger log = LoggerFactory.getLogger(GoogleRequestGenerator.class);

  GoogleTracePlayer player;

  public GoogleRequestGenerator(String name, String schedulerAddress, String experiment, long experimentTime, int speed, double suppressionFactor, double timeSuppressionFactor) throws IOException {
    super(name, schedulerAddress, experiment);

    player = new GoogleTracePlayer(name, experiment, experimentTime, speed, suppressionFactor, timeSuppressionFactor);


  }

  @Override
  public ScheduleRequest getNextRequest() {
    return player.getNextJob();
  }

  public static void main(String[] args) {
    try {
      GoogleRequestGenerator requestGenerator = new GoogleRequestGenerator("google", "127.0.0.1", "test", 100000, 10, 1.0, 1.0);
      while (true) {
        ScheduleRequest request = requestGenerator.getNextRequest();
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
