package cluster;

import com.google.common.collect.Sets;
import edu.illinois.cs.srg.cluster.Cluster;
import edu.illinois.cs.srg.scheduler.ScheduleRequest;
import edu.illinois.cs.srg.scheduler.ScheduleResponse;
import edu.illinois.cs.srg.scheduler.Task;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.*;

/**
 * Created by read on 10/23/14.
 */
public class ClusterTest {

  @Test
  public void clusterShouldNotScheduleTask() {

    // Cluster is size 0
    Cluster cluster = new Cluster(0);

    Set<Task> tasks = Sets.newHashSet();
    tasks.add(new Task(0, 0.25, 0.25));
    tasks.add(new Task(1, 0.25, 0.25));
    long id = 1;
    ScheduleRequest req = new ScheduleRequest(id, tasks);

    assertEquals("Cluster should not schedule tasks", cluster.schedule(req), false);


    // Cluster has only two machines and they are full
    Cluster cluster2 = new Cluster(2);

    Set<Task> tasks2 = Sets.newHashSet();
    tasks.add(new Task(0, 0.25, 0.25));
    tasks.add(new Task(1, 0.25, 0.25));
    tasks.add(new Task(2, 0.25, 0.25));
    tasks.add(new Task(3, 0.25, 0.25));
    tasks.add(new Task(4, 0.25, 0.25));
    long id2 = 1;
    ScheduleRequest req2 = new ScheduleRequest(id, tasks);

    assertEquals("Cluster should schedule not tasks", cluster2.schedule(req2), false);
  }

  @Test
  public void clusterShouldScheduleTasks() {
    Cluster cluster = new Cluster(2);

    Set<Task> tasks = Sets.newHashSet();
    tasks.add(new Task(0, 0.25, 0.25));
    tasks.add(new Task(1, 0.25, 0.25));
    tasks.add(new Task(1, 0.25, 0.25));
    tasks.add(new Task(1, 0.25, 0.25));
    long id = 1;
    ScheduleRequest req = new ScheduleRequest(id, tasks);

    assertEquals("Cluster should schedule tasks", cluster.schedule(req), true);
  }

  @Test
  public void clusterShouldUpdateUsage() {

    // Create local view of the cluster
    Cluster clusterState = new Cluster(2);

    // Create some tasks
    Set<Task> tasks = Sets.newHashSet();
    tasks.add(new Task(0, 0.25, 0.25));
    tasks.add(new Task(1, 0.25, 0.25));
    tasks.add(new Task(1, 0.25, 0.25));
    tasks.add(new Task(1, 0.25, 0.25));

    // Create a scheduler request and schedule the tasks on the cluster
    long id = 1;
    ScheduleRequest req = new ScheduleRequest(id, tasks);
    clusterState.schedule(req);

    // Create a "remote" view of the cluster
    Cluster remoteClusterState = new Cluster(2);
    assertEquals("Cluster state should update it's usage successfully",
            remoteClusterState.updateUsage(clusterState.getUsage()),
            true);

    //TODO: This is succeeding when it shouldn't be.
    assertEquals("Local State should reflect remote state", remoteClusterState.getUsage(),
            clusterState.getUsage());
  }
}
