package edu.illinois.cs.srg.workload.yarn;

import edu.illinois.cs.srg.YarnScheduleResponse;
import edu.illinois.cs.srg.scheduler.jobHandlers.ConsistentClusterStateJobHandler;
import edu.illinois.cs.srg.util.Constants;
import edu.illinois.cs.srg.workload.WorkloadGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by gourav on 3/20/15.
 */
public class YarnResponseServer implements Runnable {
  private final static Logger log = LoggerFactory.getLogger(YarnResponseServer.class);

  public static Map<Long, Long> waitingJobs = new ConcurrentHashMap<Long, Long>();

  protected String name;
  protected String experiment;

  public static BufferedWriter jobWriter;
  public static BufferedWriter taskWriter;
  protected Object writerLock;

  int errors;
  Object errorLock;

  protected String logdir;

  public YarnResponseServer(String name, String experiment) throws IOException {
    this.name = name;
    this.experiment = experiment;

    logdir = System.getProperty("user.home") + "/logs/" + experiment + "/";

    File jobFile = new File(logdir + "/" + name + ".job");
    jobFile.createNewFile();
    jobWriter = new BufferedWriter(new FileWriter(jobFile));

    File taskFile = new File(logdir + "/" + name + ".task");
    taskFile.createNewFile();
    taskWriter = new BufferedWriter(new FileWriter(taskFile));
    writerLock = new Object();

    errors = 0;
    errorLock = new Object();
  }

  @Override
  public void run() {
    try {
      ServerSocket serverSocket = new ServerSocket(YarnScheduleResponse.RESPONSE_SERVER_PORT);

      while (!WorkloadGenerator.terminate) {
        Socket connectionSocket = serverSocket.accept();

        // multipath
        Thread responseWriter = new Thread(new YarnResponseWriter(connectionSocket, this));
        responseWriter.start();
      }
      log.error("{} is shutting down.", this);
      try {
        Thread.sleep(Constants.WORKLOAD_SIGTERM_WAIT);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      //log.error("{} is shutting down.", this);
      jobWriter.close();
      taskWriter.close();
    } catch (Exception e) {
      log.error("{} is shutting down.", this);
      //e.printStackTrace();
    }
  }

  @Override
  public String toString() {
    return "YarnResponseServer";
  }


}
