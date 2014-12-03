package edu.illinois.cs.srg.scheduler;

import com.google.common.collect.Maps;
import edu.illinois.cs.srg.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by gourav on 10/17/14.
 */
public class JobServer implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(JobServer.class);

  ClusterState clusterState;
  Map<Long, Long> throughput;


  public final int PATH_LIMIT = 3;

  ExecutorService service;

  public JobServer(ClusterState clusterState) {
    this.clusterState = clusterState;
    throughput = Maps.newHashMap();

    service = Executors.newFixedThreadPool(PATH_LIMIT);
  }

  @Override
  public void run() {
    try {
      ServerSocket welcomeSocket = new ServerSocket(Constants.JOB_SERVER_PORT);

      while (!Scheduler.terminate) {
        Socket connectionSocket = welcomeSocket.accept();
        Debugger.addRequest();

        // fixed-paths
       /*AbstractJobHandler jobHandler = new BasicJobHandler(clusterState, connectionSocket);
        try {
          service.execute(jobHandler);
        } catch (Exception e) {
          e.printStackTrace();
          try {
            connectionSocket.close();
          } catch (IOException e1) {
            // ignore
          }
        }*/

        // multipath
        Thread jobHandler = new Thread(new BasicJobHandler(clusterState, connectionSocket));
        jobHandler.start();
      }
    } catch (Exception e) {
      log.error("{} is shutting down.", this);
      //e.printStackTrace();
    }
    service.shutdown();
  }

  @Override
  public String toString() {
    return "JobServer";
  }

  public static void main(String args[]) throws Exception {
    JobServer server = new JobServer(new ClusterState());
    server.run();
  }
}
