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

  public JobThread(ScheduleRequest request, AbstractRequestGenerator requestGenerator) {
    this.request = request;
    this.requestGenerator = requestGenerator;

  }

  @Override
  public void run() {
    long sentTime = System.currentTimeMillis();
    ScheduleResponse response = null;

    try {
      response = sendRequest(request);
      if (request.getTasks().size() != response.getSentTime().size()) {
        log.error("{}: response size does not match request size");
      }
    } catch (IOException e) {
      response = ScheduleResponse.createFailedResponse(request);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    long receiveTime = System.currentTimeMillis();
    try {
      write(response, sentTime, receiveTime);
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
    outStream.writeObject(request);
    outStream.flush();

    ScheduleResponse response = (ScheduleResponse) inStream.readObject();

    //log.info("Response: {}", response);
    outStream.close();
    inStream.close();
    clientSocket.close();
    return response;
  }

  public void write(ScheduleResponse response, long sentTime, long receiveTime) throws IOException {
    StringBuilder job = new StringBuilder(response.getJobID() + ", ").append(response.getResults().size() + ", ");
    Map<Integer, StringBuilder> tasks = Maps.newHashMap();

    long maxSentTime = 0;
    boolean jobResult = (response.getResult() == ScheduleResponse.SUCCESS);
    long maxRecvTime = 0;

    for (Map.Entry<Integer, Long> entry : response.getSentTime().entrySet()) {
      int index = entry.getKey();
      StringBuilder task = new StringBuilder(response.getJobID() + ", ").append(index + ", ").append(sentTime + ", ").append(response.getSubmissionTime() + ", ").append(entry.getValue() + ", ");

      maxSentTime = Math.max(maxSentTime, entry.getValue());

      if (response.getResults().containsKey(index)) {
        task.append(response.getResults().get(index) + ", ");
        jobResult &= response.getResults().get(index);
      } else {
        task.append(false + ", ");
      }

      if (response.getReceiveTime().containsKey(index)) {
        task.append(response.getReceiveTime().get(index) + ", ");
        maxRecvTime = Math.max(maxRecvTime, response.getReceiveTime().get(index));
      } else {
        task.append(0 + ", ");
      }

      task.append(receiveTime);
      tasks.put(index, task);

    }
    job.append(sentTime + ", ").append(response.getSubmissionTime() + ", ").append(maxSentTime + ", ").append(jobResult + ", ").append(maxRecvTime + ", ").append(receiveTime);

    synchronized (requestGenerator.writerLock) {
      requestGenerator.jobWriter.write(job.toString());
      requestGenerator.jobWriter.newLine();

      for (StringBuilder task : tasks.values()) {
        requestGenerator.taskWriter.write(task.toString());
        requestGenerator.taskWriter.newLine();
      }
    }
  }
}
