package edu.illinois.cs.srg.workload.google;

import java.util.List;
import java.util.Set;

/**
 * Created by gourav on 11/25/14.
 *
 * In general, GoogleJob stores all times in micro-seconds.
 * 
 */
public class GoogleJob implements Comparable<GoogleJob> {

  long id;

  // timestamp is in microseconds from the start
  long timestamp;
  int ntasks;

  double cpu;
  double memory;

  // duration is in micro-seconds
  Set<Long> durations;

  List<Set<ConstraintInfo>> cons;



  public GoogleJob(long id, long timestamp, int ntasks, double cpu, double memory, Set<Long> durations, List<Set<ConstraintInfo>> cons) {
    this.id = id;
    this.timestamp = timestamp;
    this.ntasks = ntasks;
    this.cpu = cpu;
    this.memory = memory;
    this.durations = durations;
    this.cons = cons;
  }

  public long getId() {
    return id;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public int getNtasks() {
    return ntasks;
  }

  public double getCpu() {
    return cpu;
  }

  public double getMemory() {
    return memory;
  }

  public Set<Long> getDurations() {
    return durations;
  }

  public List<Set<ConstraintInfo>> getCons() {
    return cons;
  }

  @Override
  public int compareTo(GoogleJob other) {
    if (other.timestamp == timestamp) {
      return 0;
    } else if (other.timestamp < timestamp){
      return 1;
    } else {
      return -1;
    }
  }

  @Override
  public String toString() {
    return "(" + id  + "," + timestamp + ")";
  }
}
