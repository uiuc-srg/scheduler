package edu.illinois.cs.srg.cluster.node;

import edu.illinois.cs.srg.cluster.ClusterEmulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by gourav on 11/14/14.
 */
public class Killer implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Node.class);

  Node node;

  public Killer(Node node) {
    this.node = node;
  }

  @Override
  public void run() {
    while(!ClusterEmulator.terminate) {
      synchronized (node.resourceLock) {

        // kill
        while (node.tasks.size() > 0 && node.tasks.peek().endTimestamp <= System.currentTimeMillis()) {
          Task task = node.tasks.poll();
          //LOG.debug("Killed task {}", task);
          node.release(task.memory, task.cpu);
        }

        // sleep
        try {
          if (node.tasks.size() > 0) {
            node.resourceLock.wait(node.tasks.peek().endTimestamp - System.currentTimeMillis());
          } else {
            node.resourceLock.wait();
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
