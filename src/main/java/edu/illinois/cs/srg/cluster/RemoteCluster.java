package edu.illinois.cs.srg.cluster;

import edu.illinois.cs.srg.scheduler.ScheduleResponse;

import java.util.List;
import java.util.Map;

/**
 * Created by read on 10/23/14.
 */
public class RemoteCluster {
  private SimpleClusterState simpleClusterStateState;

  public RemoteCluster(SimpleClusterState simpleClusterState) {
    simpleClusterStateState = simpleClusterState;
  }

  public RemoteCluster() {
    simpleClusterStateState = new SimpleClusterState();
  }

  public ScheduleResponse placeJobs(Map<Long, Usage> placement) {
    simpleClusterStateState.updateUsage(placement);
    // TODO: Add logic to set usage here
    return new ScheduleResponse(0);
  }
}
