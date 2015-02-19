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

  long submissionTime;
  Map<Integer, Long> sentTime;
  Map<Integer, Long> receiveTime;

  //TODO: update tries
  Map<Integer, Integer> tries;

  public ScheduleResponse(long jobID, long submissionTime) {
    this.jobID = jobID;
    this.submissionTime = submissionTime;
  }

  public void addResult( Map<Integer, Boolean> results, Map<Integer, Long> sentTime, Map<Integer,Long> receiveTime, int result, Map<Integer, Integer> tries) {
    this.results = results;
    this.sentTime = sentTime;
    this.receiveTime = receiveTime;
    this.result = result;
    this.tries = tries;
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

  public long getSubmissionTime() {
    return submissionTime;
  }

  public Map<Integer, Long> getSentTime() {
    return sentTime;
  }

  public Map<Integer, Long> getReceiveTime() {
    return receiveTime;
  }

  public Map<Integer, Integer> getTries() {
    return tries;
  }

  @Override
  public String toString() {
    return new StringBuilder("ScheduleResponse[").append(jobID).append(", ")
      .append(result).append("]").append(results).toString();
  }

  public static ScheduleResponse createFailedResponse(ScheduleRequest request) {
    ScheduleResponse response = new ScheduleResponse(request.jobID, 0);
    Map<Integer, Boolean> results = Maps.newHashMap();
    Map<Integer, Long> sentTime = Maps.newHashMap();
    Map<Integer, Long> receiveTime = Maps.newHashMap();
    Map<Integer, Integer> tries = Maps.newHashMap();
    for (int index : request.getTasks().keySet()) {
      results.put(index, false);
      receiveTime.put(index, new Long(0));
      sentTime.put(index, new Long(0));
      tries.put(index, 0);
    }
    response.addResult(results, sentTime, receiveTime, FAIL, tries);
    return response;
  }
}
