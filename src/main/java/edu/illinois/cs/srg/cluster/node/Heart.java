package edu.illinois.cs.srg.cluster.node;

import edu.illinois.cs.srg.serializables.Heartbeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by gourav on 11/14/14.
 */
public class Heart implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Node.class);

  Node node;

  public Heart(Node node) {
    this.node = node;
  }

  // No lock for resources - lets see what happens ?
  @Override
  public void run() {
    try {
      while (true) {

        //sleep
        try {
          Thread.sleep(Constants.HEARTBEAT_INTERVAL);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        // beat
        synchronized (node.connectionLock) {
          Heartbeat heartbeat = new Heartbeat(node.getAvailableCPU(), node.getAvailableMemory());
          LOG.debug("Sending " + heartbeat);
          node.output.writeObject(heartbeat);
          node.output.flush();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
