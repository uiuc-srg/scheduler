package edu.illinois.cs.srg.workload;

import edu.illinois.cs.srg.serializables.ScheduleRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by gourav on 11/22/14.
 */
public abstract class AbstractRequestGenerator implements Runnable {
  private static final int MAX_ERROR = 5;
  private static final long FINISH_WAIT = 5000;

  private final Logger log;

  String name;
  String schedulerAddress;
  String experiment;

  BufferedWriter jobWriter;
  BufferedWriter taskWriter;
  Object writerLock;

  int errors;
  Object errorLock;


  protected AbstractRequestGenerator(String name, String schedulerAddress, String experiment) throws IOException {
    log = LoggerFactory.getLogger(name);
    this.name = name;
    this.schedulerAddress = schedulerAddress;
    this.experiment = experiment;

    File jobFile = new File("~/logs/" + experiment + "/" + name + ".job");
    jobFile.createNewFile();
    jobWriter = new BufferedWriter(new FileWriter(jobFile));

    File taskFile = new File("~/logs/" + experiment + "/" + name + ".task");
    taskFile.createNewFile();
    taskWriter = new BufferedWriter(new FileWriter(taskFile));
    writerLock = new Object();

    errors = 0;
    errorLock = new Object();
  }

  /**
   * sleep and send job
   * @return
   */
  public abstract ScheduleRequest getNextRequest();

  @Override
  public void run() {
    try {
      while (errors < MAX_ERROR) {
        ScheduleRequest request = getNextRequest();
        if (request == null) {
          break;
        }
        Thread job = new Thread(new Job(request, this));
        job.start();
      }
      try {
        Thread.sleep(FINISH_WAIT);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      jobWriter.close();
      taskWriter.close();
    } catch(IOException e) {
      e.printStackTrace();
    }
  }


}