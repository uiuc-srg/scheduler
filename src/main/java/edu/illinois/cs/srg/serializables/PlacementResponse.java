package edu.illinois.cs.srg.serializables;

import java.io.Serializable;

/**
 * Created by read on 10/24/14.
 */
public class PlacementResponse implements Serializable {
  long jobID;
  int index;
  boolean result;
  long receiveTime;

  public PlacementResponse(long jobID, int index, long receiveTime) {
    this.jobID = jobID;
    this.index = index;
    this.receiveTime =receiveTime;
  }

  public long getJobID() {
    return jobID;
  }

  public int getIndex() {
    return index;
  }

  public boolean getResult() {
    return result;
  }

  public long getReceiveTime() {
    return receiveTime;
  }

  public void addResult(boolean result) {
    this.result = result;
  }

  @Override
  public String toString() {
    return new StringBuilder("PlacementResponse[").append(jobID).append(", ").append(index)
      .append(", ").append(result).append("]").toString();
  }
}
