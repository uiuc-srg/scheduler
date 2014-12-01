package edu.illinois.cs.srg.cluster.node;

import edu.illinois.cs.srg.cluster.ClusterEmulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by gourav on 11/14/14.
 */
public class Killer implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Node.class);

  Node node;


  public Killer(Node node) throws IOException {
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
          long actualDuration = System.currentTimeMillis() - task.startTime;
          try {
            synchronized (ClusterEmulator.killLock) {
              ClusterEmulator.kills.write(actualDuration + ", " + task.duration);
              ClusterEmulator.kills.newLine();
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
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
