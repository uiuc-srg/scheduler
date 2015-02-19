package edu.illinois.cs.srg.workload.google;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by gourav on 12/3/14.
 */
public class ConstraintReader {
  private static final Logger LOG = LoggerFactory.getLogger(ConstraintReader.class);

  String constraintFile;
  private BufferedReader reader;

  private long id = 0;
  private List<Set<ConstraintInfo>> allTasksCons;

  long jobsRead = 0;
  long linesRead = 0;

  public ConstraintReader(String constraintFile) throws IOException {
    this.constraintFile = constraintFile;
    reader = new BufferedReader(new FileReader(constraintFile));
    id = read();

  }

  public List<Set<ConstraintInfo>> getNextConstraint(long jobid) throws IOException {
    if (id == 0) {
      return new ArrayList<Set<ConstraintInfo>>();
    }
    if (id == jobid) {
      List<Set<ConstraintInfo>> tobeRet = allTasksCons;
      id = read();
      return tobeRet;
    } else {
      return new ArrayList<Set<ConstraintInfo>>();
    }
  }

  private long read() throws IOException {
    String jobLine = reader.readLine();
    if (jobLine == null) {
      return 0;
    }
    linesRead++;
    jobsRead++;

    allTasksCons = new ArrayList<Set<ConstraintInfo>>();
    String[] job = jobLine.split(", ", -1);
    long jobID = Long.parseLong(job[0]);
    int numberOfLines = Integer.parseInt(job[1]);

    for (int i=0; i<numberOfLines; i++) {
      String consLine = reader.readLine();
      linesRead++;
      String[] cons = consLine.split(", ", -1);
      if (Long.parseLong(cons[0]) != jobID) {
        LOG.error("Mismatch - job id from job and cons line.");
        return 0;
      }
      allTasksCons.add(readTask(cons));

    }
    return jobID;
  }

  private Set<ConstraintInfo> readTask(String[] constraints) {
    Set<ConstraintInfo> taskConstraints = Sets.newHashSet();
    for (int i=1; i<constraints.length; i=i+3) {
      taskConstraints.add(new ConstraintInfo(constraints[i], Integer.parseInt(constraints[i+1]), constraints[i+2]));
    }
    return taskConstraints;
  }

  public void close() throws IOException {
    LOG.info("lines read {} , jobs Read {}", linesRead, jobsRead);
    if (reader.readLine() != null) {
      LOG.error("All cons were not read");
    }
    reader.close();
  }
}
