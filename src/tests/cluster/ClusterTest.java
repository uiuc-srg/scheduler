package cluster;

import com.google.common.collect.Sets;
import edu.illinois.cs.srg.cluster.SimpleClusterState;
import edu.illinois.cs.srg.serializables.ScheduleRequest;
import edu.illinois.cs.srg.scheduler.Task;
import org.junit.Test;

import java.util.Set;

/**
 * Created by read on 10/23/14.
 */
//TODO: Change this to scheduler test
public class ClusterTest {

  @Test
  public void clusterShouldNotScheduleTask() {

    // Cluster is size 0
    SimpleClusterState simpleClusterState = new SimpleClusterState(0);

    Set<Task> tasks = Sets.newHashSet();
    tasks.add(new Task(0, 0.25, 0.25));
    tasks.add(new Task(1, 0.25, 0.25));
    long id = 1;
    ScheduleRequest req = new ScheduleRequest(id, tasks);

    assertEquals("Cluster should not schedule tasks", simpleClusterState.schedule(req), false);


    // Cluster has only two machines and they are full
    SimpleClusterState simpleClusterState2 = new SimpleClusterState(2);

    Set<Task> tasks2 = Sets.newHashSet();
    tasks.add(new Task(0, 0.25, 0.25));
    tasks.add(new Task(1, 0.25, 0.25));
    tasks.add(new Task(2, 0.25, 0.25));
    tasks.add(new Task(3, 0.25, 0.25));
    tasks.add(new Task(4, 0.25, 0.25));
    long id2 = 1;
    ScheduleRequest req2 = new ScheduleRequest(id, tasks);

    assertEquals("Cluster should schedule not tasks", simpleClusterState2.schedule(req2), false);
  }

  @Test
  public void clusterShouldScheduleTasks() {
    SimpleClusterState simpleClusterState = new SimpleClusterState(2);

    Set<Task> tasks = Sets.newHashSet();
    tasks.add(new Task(0, 0.25, 0.25));
    tasks.add(new Task(1, 0.25, 0.25));
    tasks.add(new Task(1, 0.25, 0.25));
    tasks.add(new Task(1, 0.25, 0.25));
    long id = 1;
    ScheduleRequest req = new ScheduleRequest(id, tasks);

    assertEquals("Cluster should schedule tasks", simpleClusterState.schedule(req), true);
  }

  @Test
  public void clusterShouldUpdateUsage() {

    // Create local view of the cluster
    SimpleClusterState simpleClusterStateState = new SimpleClusterState(2);

    // Create some tasks
    Set<Task> tasks = Sets.newHashSet();
    tasks.add(new Task(0, 0.25, 0.25));
    tasks.add(new Task(1, 0.25, 0.25));
    tasks.add(new Task(1, 0.25, 0.25));
    tasks.add(new Task(1, 0.25, 0.25));

    // Create a scheduler request and schedule the tasks on the cluster
    long id = 1;
    ScheduleRequest req = new ScheduleRequest(id, tasks);
    simpleClusterStateState.schedule(req);

    // Create a "remote" view of the cluster
    SimpleClusterState remoteSimpleClusterStateState = new SimpleClusterState(2);
    assertEquals("Cluster state should update it's usage successfully",
            remoteSimpleClusterStateState.updateUsage(simpleClusterStateState.getUsage()),
            true);

    //TODO: This is succeeding when it shouldn't be.
    assertEquals("Local State should reflect remote state", remoteSimpleClusterStateState.getUsage(),
            simpleClusterStateState.getUsage());
  }
}
