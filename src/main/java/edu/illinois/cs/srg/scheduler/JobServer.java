package edu.illinois.cs.srg.scheduler;

import edu.illinois.cs.srg.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by gourav on 10/17/14.
 */
public class JobServer implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(JobServer.class);

  @Override
  public void run() {
    try {
      ServerSocket welcomeSocket = new ServerSocket(Constants.SCHEDULER_PORT);

      while (true) {
        Socket connectionSocket = welcomeSocket.accept();
        Thread scheduler = new Thread(new DefaultJobHandler(connectionSocket));
        // multipath
        scheduler.start();
      }
    } catch (Exception e) {
      log.error("SchedulerServer is shutting down.");
      e.printStackTrace();
    }
  }

  public static void main(String args[]) throws Exception {
    JobServer server = new JobServer();
    server.run();
  }
}
