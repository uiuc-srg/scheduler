package edu.illinois.cs.srg.cluster.node;

/**
 * Created by gourav on 11/14/14.
 */
public class Killer implements Runnable {

  Node node;

  public Killer(Node node) {
    this.node = node;
  }

  @Override
  public void run() {
    while(true) {
      synchronized (node.resourceLock) {

        // kill
        while (node.tasks.size() > 0 && node.tasks.peek().endTimestamp >= System.currentTimeMillis()) {
          Task task = node.tasks.poll();
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
