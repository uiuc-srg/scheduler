package edu.illinois.cs.srg.serializables;

import java.io.Serializable;

/**
 * Created by gourav on 2/28/15.
 */
public class AbstractRequest implements Serializable {
  protected long jobID;
  protected int index;

  public AbstractRequest(long jobID, int index) {
    this.jobID = jobID;
    this.index = index;
  }

  public long getJobID() {
    return jobID;
  }

  public int getIndex() {
    return index;
  }

  @Override
  public String toString() {
    return new StringBuilder("PlacementRequest[").append(jobID).append("]").toString();
  }
}
