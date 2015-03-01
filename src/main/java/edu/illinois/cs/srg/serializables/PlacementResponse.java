package edu.illinois.cs.srg.serializables;

import java.io.Serializable;

/**
 * Created by read on 10/24/14.
 */
public class PlacementResponse implements Serializable {
  long jobID;
  int index;
  boolean result;

  long recvCluster;
  long sentCluster;

  long sentSchedulerCluster;
  long recvSchedulerCluster;

  public PlacementResponse(long jobID, int index, long receiveTime) {
    this.jobID = jobID;
    this.index = index;
    this.recvCluster = receiveTime;
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

  public long getRecvCluster() {
    return recvCluster;
  }

  public long getSentCluster() {
    return sentCluster;
  }

  public void addResult(boolean result) {
    this.result = result;
  }

  public void setSentCluster(long sentCluster) {
    this.sentCluster = sentCluster;
  }

  public long getSentSchedulerCluster() {
    return sentSchedulerCluster;
  }

  public long getRecvSchedulerCluster() {
    return recvSchedulerCluster;
  }

  public void setSentSchedulerCluster(long sentSchedulerCluster) {
    this.sentSchedulerCluster = sentSchedulerCluster;
  }

  public void setRecvSchedulerCluster(long recvSchedulerCluster) {
    this.recvSchedulerCluster = recvSchedulerCluster;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public void setResult(boolean result) {
    this.result = result;
  }

  @Override
  public String toString() {
    return new StringBuilder("PlacementResponse[").append(jobID).append(", ").append(index)
      .append(", ").append(result).append("]").toString();
  }
}
