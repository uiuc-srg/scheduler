package edu.illinois.cs.srg.workload;

import edu.illinois.cs.srg.serializables.ScheduleRequest;
import edu.illinois.cs.srg.util.Constants;
import edu.illinois.cs.srg.workload.google.GoogleRequestGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Created by gourav on 10/17/14.
 */
public class WorkloadGenerator {
  private static final Logger log = LoggerFactory.getLogger(WorkloadGenerator.class);

  public static boolean terminate = false;

  String schedulerAddress;
  String experiment;
  Thread monitor;

  public WorkloadGenerator(String experiment, String schedulerAddress) {
    this.experiment = experiment;
    this.schedulerAddress = schedulerAddress;
  }

  public void generate() {
    try {
      String logdir = System.getProperty("user.home") + "/logs/" + experiment;
      File dir = new File(logdir);
      dir.mkdirs();

      monitor = new Thread(new WGMonitor(logdir + "/monitor"));
      monitor.start();


      //Thread requestGenerator = new Thread(new DefaultRequestGenerator("default", schedulerAddress, experiment));
      Thread requestGenerator = new Thread(new GoogleRequestGenerator("google", schedulerAddress, experiment, 4*60*60*1000, 100));
      requestGenerator.start();
      requestGenerator.join();

    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    exit();
  }

  public void exit() {
    try {
      Socket clientSocket = new Socket(schedulerAddress, Constants.JOB_SERVER_PORT);
      ObjectOutputStream outStream = new ObjectOutputStream(clientSocket.getOutputStream());
      outStream.flush();
      ScheduleRequest request = Constants.createSIGTERMScheduleRequest();
      log.info("Request: {}", request);
      outStream.writeObject(request);
      outStream.flush();

      try {
        Thread.sleep(Constants.WORKLOAD_SIGTERM_WAIT);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      outStream.close();
      clientSocket.close();
    } catch (IOException e) {
      log.error("Unable to terminate experiment gracefully");
      e.printStackTrace();
    }

    log.info("Shutting down WorkloadGenerator");
    WorkloadGenerator.terminate = true;
  }



  public static void main(String args[])  {
    log.info("Starting WorkloadGenerator.");
    String schedulerAddress = "127.0.0.1";
    if (args.length > 1) {
      schedulerAddress = args[1];
    }
    WorkloadGenerator generator = new WorkloadGenerator(args[0], schedulerAddress);
    generator.generate();
  }
}
