package edu.illinois.cs.srg.workload;

import com.google.common.collect.Maps;
import edu.illinois.cs.srg.serializables.ScheduleRequest;
import edu.illinois.cs.srg.serializables.ScheduleResponse;
import edu.illinois.cs.srg.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Map;

/**
 * Created by gourav on 11/2/14.
 */
public class JobThread implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(WorkloadGenerator.class);

  AbstractRequestGenerator requestGenerator;
  ScheduleRequest request;

  long sentTimeGlobal;
  long receiveTimeGlobal;

  public JobThread(ScheduleRequest request, AbstractRequestGenerator requestGenerator) {
    this.request = request;
    this.requestGenerator = requestGenerator;
  }

  @Override
  public void run() {
    ScheduleResponse response = null;
    try {
      response = sendRequest(request);
      if (request.getTasks().size() != response.getResults().size()) {
        log.error("{}: response size does not match request size");
      }
    } catch (IOException e) {
      response = ScheduleResponse.createFailedResponse(request);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    try {
      write(response, sentTimeGlobal, receiveTimeGlobal);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public ScheduleResponse sendRequest(ScheduleRequest request) throws IOException, ClassNotFoundException {
    Socket clientSocket = new Socket(requestGenerator.schedulerAddress, Constants.JOB_SERVER_PORT);
    ObjectOutputStream outStream = new ObjectOutputStream(clientSocket.getOutputStream());
    outStream.flush();
    ObjectInputStream inStream = new ObjectInputStream(clientSocket.getInputStream());
    //log.info("Request: {}", request);
    sentTimeGlobal = System.currentTimeMillis();
    outStream.writeObject(request);
    outStream.flush();

    ScheduleResponse response = (ScheduleResponse) inStream.readObject();
    receiveTimeGlobal = System.currentTimeMillis();

    //log.info("Response: {}", response);
    outStream.close();
    inStream.close();
    clientSocket.close();
    return response;
  }

  private long maxTime(Map<Integer, Long> times) {
    long max = 0;
    for (long time : times.values()) {
      max = Math.max(max, time);
    }
    return max;
  }

  private int maxAttempts(Map<Integer, Integer> attempts) {
    int max = 0;
    for (int attempt : attempts.values()) {
      max = Math.max(max, attempt);
    }
    return max;
  }

  private boolean andResult(Map<Integer, Boolean> results) {
    boolean and = true;
    for (boolean result : results.values()) {
      and = and && result;
    }
    return and;
  }


  public void write(ScheduleResponse response, long sentTime, long receiveTime) throws IOException {
    long maxSentSchedulerCluster = maxTime(response.getSentCluster());
    long maxRecvCluster = maxTime(response.getRecvCluster());
    long maxSentCluster = maxTime(response.getSentCluster());
    long maxRecvSchedulerCluster = maxTime(response.getRecvSchedulerCluster());
    boolean result = andResult(response.getResults()) && (response.getResult() == ScheduleResponse.SUCCESS);
    int maxTries = maxAttempts(response.getTries());

    String job = new StringBuilder(
      response.getJobID() + ", ")
      .append(response.getResults().size() + ", ")
      .append(sentTime + ", ")
      .append(response.getRecvSchedulerWG() + ", ")
      .append(maxSentSchedulerCluster + ", ")
      .append(maxRecvCluster + ", ")
      .append(maxSentCluster + ", ")
      .append(maxRecvSchedulerCluster +", ")
      .append(response.getSentSchedulerWG() + ", ")
      .append(receiveTime + ", ")
      .append(result + ", ")
      .append(maxTries)
      .toString();

    Map<Integer, String> tasks = Maps.newHashMap();
    for (int index : response.getResults().keySet()) {
      String task = new StringBuilder(
        response.getJobID() + ", ")
        .append(index + ", ")
        .append(sentTime + ", ")
        .append(response.getRecvSchedulerWG() + ", ")
        .append(response.getSentSchedulerCluster().get(index)+ ", ")
        .append(response.getRecvCluster().get(index) + ", ")
        .append(response.getSentCluster().get(index) + ", ")
        .append(response.getRecvSchedulerCluster().get(index) + ", ")
        .append(response.getSentSchedulerWG() + ", ")
        .append(receiveTime + ", ")
        .append(response.getResults().get(index) + ", ")
        .append(response.getTries().get(index))
        .toString();

      tasks.put(index, task);
    }

    synchronized (requestGenerator.writerLock) {
      requestGenerator.jobWriter.write(job);
      requestGenerator.jobWriter.newLine();

      for (String task : tasks.values()) {
        requestGenerator.taskWriter.write(task);
        requestGenerator.taskWriter.newLine();
      }
    }
  }
}
