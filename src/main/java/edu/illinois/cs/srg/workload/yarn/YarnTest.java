package edu.illinois.cs.srg.workload.yarn;

import com.google.common.collect.Maps;
import edu.illinois.cs.srg.scheduler.TaskInfo;
import edu.illinois.cs.srg.serializables.ScheduleRequest;
import edu.illinois.cs.srg.util.Constants;
import edu.illinois.cs.srg.workload.AbstractRequestGenerator;
import edu.illinois.cs.srg.workload.google.GoogleTracePlayer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Created by gourav on 3/22/15.
 */
public class YarnTest implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(YarnRequestGenerator.class);

  YarnResponseServer responseServer;

  protected String name;
  protected String rmAddress;
  protected String myAddress;
  protected String mustangNM;
  protected String experiment;

  public YarnTest(String name, String rmAddress, String myAddress, String mustangNM, String experiment, long experimentTime, int speed, double suppressionFactor) throws IOException {
    this.name = name;
    this.rmAddress = rmAddress;
    this.myAddress = myAddress;
    this.mustangNM = mustangNM;
    this.experiment = experiment;

    responseServer = new YarnResponseServer(name, experiment);
    Thread responseServerThread = new Thread(responseServer);
    responseServerThread.start();
  }

  @Override
  public void run() {

    Map<Integer, Long> durations = Maps.newHashMap();
    double cpu = 0.5;
    double mem = 0.5;
    durations.put(0, new Long(1000));

    Thread client = new Thread(new Client(rmAddress, myAddress, mustangNM, 1, 1, cpu, mem, durations));
    client.start();

    log.info("YarnTest is shutting down.");
    try {
      Thread.sleep(2* Constants.TIMEOUT);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {

  }

}
