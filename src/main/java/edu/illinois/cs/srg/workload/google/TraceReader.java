package edu.illinois.cs.srg.workload.google;


import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Created by gourav on 11/2/14.
 */
public class TraceReader {
  private static final Logger log = LoggerFactory.getLogger(TraceReader.class);

  String attributesFile;
  String durationFile;



  public TraceReader(String attributesFile, String durationFile) {
    this.attributesFile = attributesFile;
    this.durationFile = durationFile;

  }

  public List<GoogleJob> getJobs() {

    List<GoogleJob> googleJobs = Lists.newArrayList();

    try {
      BufferedReader attributesReader = new BufferedReader(new FileReader(new File(attributesFile)));
      BufferedReader durationReader = new BufferedReader(new FileReader(new File(durationFile)));

      String attributeLine;
      String durationLine;
      int missed = 0;

      while ((attributeLine = attributesReader.readLine()) != null && (durationLine = durationReader.readLine()) != null) {

        // create job
        String[] attribute = attributeLine.split(" *, *");
        String[] duration = durationLine.split(" *, *");
        long id = Long.parseLong(attribute[0]);
        long idFromDuration = Long.parseLong(duration[0]);
        int numberOfTasks = (int) Double.parseDouble(attribute[1]);
        long arrival = (long) Double.parseDouble(attribute[2]);

        double cpu = Double.parseDouble(attribute[14]);
        double memory = Double.parseDouble(attribute[20]);
        Set<Long> durations = Sets.newHashSet();
        for (int i = 1; i<duration.length; i++) {
          durations.add(Long.parseLong(duration[i]));
        }

        // error checking
        if (durations.size() == 0) {
          missed++;
          //log.warn("Skipping job {}", id);
          continue;
        }

        googleJobs.add(new GoogleJob(id, arrival, numberOfTasks, cpu, memory, durations));

        if (id != idFromDuration) {
          log.error("Mismatch in ids: {}, {}", Arrays.toString(attribute), Arrays.toString(duration));
          break;
        }
      }
      Collections.sort(googleJobs);
      attributesReader.close();
      durationReader.close();

      log.info("Missed: {}", missed);
      //log.info(googleJobs + "");

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return googleJobs;
  }


  public static void main(String[] args) {
    TraceReader reader = new TraceReader("/Users/gourav/scheduling/debug_attributes", "/Users/gourav/scheduling/debug_durationsNoNaN");
    reader.getJobs();
  }

}
