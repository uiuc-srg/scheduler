package edu.illinois.cs.srg.cluster;

import edu.illinois.cs.srg.scheduler.Task;

import java.util.List;
import java.util.Map;

/**
 * Created by read on 10/24/14.
 */
public class PlacementRequest {
    // Map < Machine-Id's, List<Task>>
    private Map<Long, List<Task>> taskPlacements;
}
