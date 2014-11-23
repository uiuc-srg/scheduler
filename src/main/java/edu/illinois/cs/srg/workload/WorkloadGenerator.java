package edu.illinois.cs.srg.workload;

import edu.illinois.cs.srg.util.Constants;
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

  String schedulerAddress;
  String experiment;

  public WorkloadGenerator(String experiment, String schedulerAddress) {
    this.experiment = experiment;
    this.schedulerAddress = schedulerAddress;
  }

  public void generate() {
    try {

      File dir = new File("~/logs/" + experiment);
      dir.mkdirs();

      Thread requestGenerator = new Thread(new DefaultRequestGenerator("default", schedulerAddress, experiment));
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
      outStream.writeObject(Constants.createSIGTERMScheduleRequest());
      outStream.flush();

      outStream.close();
      clientSocket.close();
    } catch (IOException e) {
      log.error("Unable to terminate experiment gracefully");
      e.printStackTrace();
    }
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
