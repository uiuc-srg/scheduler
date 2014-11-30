package edu.illinois.cs.srg.serializables;


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

  public ScheduleResponse(long jobID, long submissionTime) {
    this.jobID = jobID;
    this.submissionTime = submissionTime;
  }

  public void addResult( Map<Integer, Boolean> results, Map<Integer, Long> sentTime, Map<Integer,Long> receiveTime, int result) {
    this.results = results;
    this.sentTime = sentTime;
    this.receiveTime = receiveTime;
    this.result = result;
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

  @Override
  public String toString() {
    return new StringBuilder("ScheduleResponse[").append(jobID).append(", ")
      .append(result).append("]").append(results).toString();
  }
}
