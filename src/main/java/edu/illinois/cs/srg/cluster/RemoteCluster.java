package edu.illinois.cs.srg.cluster;

import edu.illinois.cs.srg.scheduler.ScheduleResponse;

import java.util.List;
import java.util.Map;

/**
 * Created by read on 10/23/14.
 */
public class RemoteCluster {
  private Cluster clusterState;

  public RemoteCluster(Cluster cluster) {
    clusterState = cluster;
  }

  public RemoteCluster() {
    clusterState = new Cluster();
  }

  public ScheduleResponse placeJobs(Map<Long, Usage> placement) {
    clusterState.updateUsage(placement);
    // TODO: Add logic to set usage here
    return new ScheduleResponse(0);
  }
}
