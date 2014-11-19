package edu.illinois.cs.srg.serializables;

import java.io.Serializable;

/**
 * Created by read on 10/24/14.
 */
public class PlacementResponse implements Serializable {
  long jobID;
  int index;
  boolean result;

  public PlacementResponse(long jobID, int index, boolean result) {
    this.jobID = jobID;
    this.index = index;
    this.result = result;
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

  @Override
  public String toString() {
    return new StringBuilder("PlacementResponse[").append(jobID).append(", ").append(index)
      .append(", ").append(result).append("]").toString();
  }
}
