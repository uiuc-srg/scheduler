package edu.illinois.cs.srg.util;

import edu.illinois.cs.srg.scheduler.TaskInfo;
import edu.illinois.cs.srg.serializables.PlacementRequest;
import edu.illinois.cs.srg.serializables.ScheduleRequest;
import edu.illinois.cs.srg.serializables.ScheduleResponse;

import java.util.HashMap;

/**
 * Created by gourav on 10/17/14.
 */
public class Constants {

  public static final int JOB_SERVER_PORT = 9999;
  public static final int NODE_SERVER_PORT = 10000;

  public static final long HEARTBEAT_INTERVAL = 5000;

  public static final double OS_CPU_FRACTION = 0.2;
  public static final double OS_MEMORY_FRACTION = 0.2;

  public static final long TIMEOUT = 5000;
  public static final long STATS_INTERVAL = 1000;

  public static final long SCHEDULER_SIGTERM_WAIT = 2000;

  public static final int SIGTERM = -1;

  public static ScheduleRequest createSIGTERMScheduleRequest() {
    return new ScheduleRequest(SIGTERM, new HashMap<Integer, TaskInfo>());
  }

  public static PlacementRequest createSIGTERMPlacementRequest() {
    return new PlacementRequest(SIGTERM, 0, null);
  }
}
