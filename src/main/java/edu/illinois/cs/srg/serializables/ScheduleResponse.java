package edu.illinois.cs.srg.serializables;


import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by gourav on 10/17/14.
 */
public class ScheduleResponse implements Serializable {

  public static final int SUCCESS = 1;
  public static final int FAIL = 0;

  long jobID;
  Map<Integer, Boolean> results;
  int result;

  long recvSchedulerWG;
  long sentSchedulerWG;

  Map<Integer, Long> sentSchedulerCluster;
  Map<Integer, Long> recvSchedulerCluster;

  Map<Integer, Long> recvCluster;
  Map<Integer, Long> sentCluster;

  Map<Integer, Integer> tries;

  public ScheduleResponse(long jobID, long recvSchedulerWG) {
    this.jobID = jobID;
    this.recvSchedulerWG = recvSchedulerWG;
  }

  public void addResult(Map<Integer, Long> sentSchedulerCluster, Map<Integer,Long> recvCluster, Map<Integer, Long> sentCluster, Map<Integer, Long> recvSchedulerCluster, Map<Integer, Boolean> results, Map<Integer, Integer> tries, int result) {
    this.sentSchedulerWG = System.currentTimeMillis();
    this.sentSchedulerCluster = sentSchedulerCluster;
    this.recvCluster = recvCluster;
    this.sentCluster = sentCluster;
    this.recvSchedulerCluster = recvSchedulerCluster;

    this.results = results;
    this.result = result;
    this.tries = tries;
  }

  public void setSentSchedulerWG(long sentSchedulerWG) {
    this.sentSchedulerWG = sentSchedulerWG;
  }

  public long getJobID() {
    return jobID;
  }

  public Map<Integer, Boolean> getResults() {
    return results;
  }

  public int getResult() {
    return result;
  }

  public long getRecvSchedulerWG() {
    return recvSchedulerWG;
  }

  public long getSentSchedulerWG() {
    return sentSchedulerWG;
  }

  public Map<Integer, Long> getSentSchedulerCluster() {
    return sentSchedulerCluster;
  }

  public Map<Integer, Long> getRecvSchedulerCluster() {
    return recvSchedulerCluster;
  }

  public Map<Integer, Long> getRecvCluster() {
    return recvCluster;
  }

  public Map<Integer, Long> getSentCluster() {
    return sentCluster;
  }

  public Map<Integer, Integer> getTries() {
    return tries;
  }

  @Override
  public String toString() {
    return new StringBuilder("ScheduleResponse[").append(jobID).append(", ")
      .append(result).append("]").append(results).toString();
  }

  // GONNA-FAIL. Called only by WG for now.
  public static ScheduleResponse createFailedResponse(ScheduleRequest request) {
    ScheduleResponse response = new ScheduleResponse(request.jobID, 0);
    Map<Integer, Long> sentSchedulerClusterTimes = Maps.newHashMap();
    Map<Integer, Long> recvClusterTimes = Maps.newHashMap();
    Map<Integer, Long> sentClusterTimes = Maps.newHashMap();
    Map<Integer, Long> recvSchedulerClusterTimes = Maps.newHashMap();

    Map<Integer, Boolean> results = Maps.newHashMap();
    Map<Integer, Integer> tries = Maps.newHashMap();

    for (int index : request.getTasks().keySet()) {
      results.put(index, false);
      sentSchedulerClusterTimes.put(index, new Long(0));
      recvClusterTimes.put(index, new Long(0));
      sentClusterTimes.put(index, new Long(0));
      recvSchedulerClusterTimes.put(index, new Long(0));
      tries.put(index, 0);
    }
    response.addResult(
      sentSchedulerClusterTimes, recvClusterTimes,
      sentClusterTimes, recvSchedulerClusterTimes,
      results, tries, FAIL);
    return response;
  }
}
