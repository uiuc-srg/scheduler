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

  public ScheduleResponse(int result) {
    this.result = result;
  }

  public ScheduleResponse(long jobID, Map<Integer, Boolean> results, int result) {
    this.jobID = jobID;
    this.results = results;
    this.result = result;
  }

  public ScheduleResponse(boolean result) {
      if (result)
          this.result = 1;
      else
          this.result = 0;
  }

  @Override
  public String toString() {
    return new StringBuilder("ScheduleResponse[").append(jobID).append(", ")
      .append(result).append("]").toString();
  }
}
