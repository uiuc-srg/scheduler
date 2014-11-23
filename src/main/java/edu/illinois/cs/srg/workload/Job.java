package edu.illinois.cs.srg.workload;

import com.google.common.collect.Maps;
import edu.illinois.cs.srg.serializables.ScheduleRequest;
import edu.illinois.cs.srg.serializables.ScheduleResponse;
import edu.illinois.cs.srg.util.Constants;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Map;

/**
 * Created by gourav on 11/2/14.
 */
public class Job implements Runnable {

  AbstractRequestGenerator requestGenerator;
  ScheduleRequest request;

  public Job(ScheduleRequest request, AbstractRequestGenerator requestGenerator) {
    this.request = request;
    this.requestGenerator = requestGenerator;

  }

  @Override
  public void run() {
    try {
      long sentTime = System.currentTimeMillis();
      ScheduleResponse response = sendRequest(request);
      long receiveTime = System.currentTimeMillis();
      write(response, sentTime, receiveTime);
    } catch (IOException e) {
      synchronized (requestGenerator.errorLock) {
        requestGenerator.errors++;
      }
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public ScheduleResponse sendRequest(ScheduleRequest request) throws IOException, ClassNotFoundException {
    Socket clientSocket = new Socket(requestGenerator.schedulerAddress, Constants.JOB_SERVER_PORT);
    ObjectOutputStream outStream = new ObjectOutputStream(clientSocket.getOutputStream());
    outStream.flush();
    ObjectInputStream inStream = new ObjectInputStream(clientSocket.getInputStream());
    outStream.writeObject(request);
    ScheduleResponse response = (ScheduleResponse) inStream.readObject();

    //log.info("Got Response: {}", response);
    outStream.close();
    inStream.close();
    clientSocket.close();
    return response;
  }

  public void write(ScheduleResponse response, long sentTime, long receiveTime) throws IOException {
    StringBuilder job = new StringBuilder(response.getJobID() + ", ").append(response.getResults().size() + ", ");
    Map<Integer, StringBuilder> tasks = Maps.newHashMap();

    boolean jobResult = (response.getResult() == ScheduleResponse.SUCCESS);
    for (Map.Entry<Integer, Boolean> entry : response.getResults().entrySet()) {
      tasks.put(entry.getKey(), new StringBuilder(response.getJobID() + ", ").append(entry.getKey() + ", ").append(entry.getValue()));
      jobResult &= entry.getValue();
    }
    job.append(jobResult + ", ");

    long maxSentTime = 0;
    for (Map.Entry<Integer, Long> entry : response.getSentTime().entrySet()) {
      tasks.get(entry.getKey()).append(sentTime + ", ").append(response.getSubmissionTime() + ", ").append(entry.getValue() + ", ");
      maxSentTime = Math.max(maxSentTime, entry.getValue());
    }
    job.append(sentTime + ", ").append(response.getSubmissionTime() + ", ").append(maxSentTime + ", ");

    long maxRecvTime = 0;
    for (Map.Entry<Integer, Long> entry : response.getReceiveTime().entrySet()) {
      tasks.get(entry.getKey()).append(entry.getValue() + ", ").append(receiveTime);
      maxRecvTime = Math.max(maxRecvTime, entry.getValue());
    }
    job.append(maxRecvTime + ", ").append(receiveTime);


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
