package edu.illinois.cs.srg.workload.google;


import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * Created by gourav on 11/2/14.
 */
public class TraceReader {
  private static final Logger log = LoggerFactory.getLogger(TraceReader.class);

  String attributesFile;
  String durationFile;

  ConstraintReader constraintReader;

  double suppressionFactor;

  public TraceReader(String attributesFile, String durationFile, String constraintFile) throws IOException {
    this.attributesFile = attributesFile;
    this.durationFile = durationFile;
    this.constraintReader = new ConstraintReader(constraintFile);
    this.suppressionFactor = 1;
  }

  public TraceReader(String attributesFile, String durationFile, String constraintFile, double suppressionFactor) throws IOException {
    this.attributesFile = attributesFile;
    this.durationFile = durationFile;
    this.constraintReader = new ConstraintReader(constraintFile);
    this.suppressionFactor = suppressionFactor;
  }

  public List<GoogleJob> getJobs() throws IOException {

    List<GoogleJob> googleJobs = Lists.newArrayList();

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

      double cpu = Double.parseDouble(attribute[14])*suppressionFactor;
      double memory = Double.parseDouble(attribute[20])*suppressionFactor;
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

      List<Set<ConstraintInfo>> cons = constraintReader.getNextConstraint(id);

      googleJobs.add(new GoogleJob(id, arrival, numberOfTasks, cpu, memory, durations, cons));

      if (id != idFromDuration) {
        log.error("Mismatch in ids: {}, {}", Arrays.toString(attribute), Arrays.toString(duration));
        break;
      }
    }
    Collections.sort(googleJobs);
    attributesReader.close();
    durationReader.close();
    constraintReader.close();

    log.info("Missed: {}", missed);
    //log.info(googleJobs + "");


    return googleJobs;
  }

  public static void main(String[] args) {
    try {
      TraceReader reader = new TraceReader(System.getProperty("user.home") + "/traces/attributes", System.getProperty("user.home") + "/traces/durationsNoNaN", System.getProperty("user.home") + "/traces/constraints");
      reader.getJobs();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
