package edu.illinois.cs.srg.workload.yarn;

import com.google.common.collect.Maps;
import edu.illinois.cs.srg.YarnScheduleResponse;
import edu.illinois.cs.srg.serializables.ScheduleRequest;
import edu.illinois.cs.srg.serializables.ScheduleResponse;
import edu.illinois.cs.srg.workload.JobThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Map;

/**
 * Created by gourav on 3/20/15.
 */
public class YarnResponseWriter implements Runnable {
  private static Logger log = LoggerFactory.getLogger(YarnResponseWriter.class);

  Socket socket;
  YarnScheduleResponse response;
  YarnResponseServer server;

  public YarnResponseWriter(Socket socket, YarnResponseServer server) {
    this.socket = socket;
    this.server = server;
  }

  @Override
  public void run() {
    try {
      ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
      outputStream.flush();
      ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
      response = (YarnScheduleResponse) inputStream.readObject();
      outputStream.close();
      inputStream.close();
      socket.close();
      //log.info("{}: Got Response {}", this, response);
      long receiveTime = System.currentTimeMillis();
      // find sent time and call write
      Long sentTime = YarnResponseServer.waitingJobs.remove(response.jobID);
      if (sentTime != null){
        write(response, sentTime, receiveTime);
      } else {
        log.error("{} cannot find waiting job {}", this, response);
        return;
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  /*

   */
  public void write(YarnScheduleResponse response, long sentTime, long receiveTime) throws IOException {
    boolean allTasksGotContainers = true;
    if (response.getNtasks() != response.getContainerAllocationTimes().size()) {
      log.error("Allocation problem: {}: ntasks {}, allocationTimeSize {} ", this, response.getNtasks(), response.getContainerAllocationTimes().size());
      synchronized (server.errorLock) {
        server.errors++;
      }
      allTasksGotContainers = false;
    }
    if (response.getContainerAllocationTimes().size() != response.getContainerStartTimes().size()
      //|| response.getContainerAllocationTimes().size() != response.getContainerStopTimes().size()
      || response.getContainerAllocationTimes().size() != response.getContainerCompletionTime().size()
      || response.getContainerAllocationTimes().size() != response.getResults().size()) {
      log.warn("Completion problem: {}: ntasks {}, allocationTimeSize {}, startTimeSize {}, stopTimeSize {}, completionTimeSize {}, resultTimeSize {}",
        this, response.getNtasks(), response.getContainerAllocationTimes().size(),
        response.getContainerStartTimes().size(), response.getContainerStopTimes().size(), response.getContainerCompletionTime().size(), response.getResults().size());
    }
    long maxContainerAllocationTime = JobThread.maxTime(response.getContainerAllocationTimes());
    long maxContainerStartTime = JobThread.maxTime(response.getContainerStartTimes());
    long maxContainerCompletionTime = JobThread.maxTime(response.getContainerCompletionTime());
    long maxDuration = JobThread.maxTime(response.getDurations());
    boolean result = JobThread.andResult(response.getResults()) && response.getResult() && allTasksGotContainers;

    String job = new StringBuilder(
      response.getJobID() + ", ")
      .append(response.getNtasks() + ", ")
      .append(sentTime + ", ")
      .append(response.getAPP_SUBMIT_TIME() + ", ")
      .append(response.getAmStartTime() + ", ")
      .append(maxContainerAllocationTime + ", ")
      .append(maxContainerStartTime + ", ")
      .append(receiveTime +", ")
      .append(result + ", ")
      .append(maxContainerCompletionTime + ", ")
      .append(maxDuration)
      .toString();

    Map<Integer, String> tasks = Maps.newHashMap();
    for (int index : response.getContainerAllocationTimes().keySet()) {
      long containerStartTime = 0;
      if (response.getContainerStartTimes().containsKey(index)) {
        containerStartTime = response.getContainerStartTimes().get(index);
      }
      boolean taskResult = false;
      if (response.getResults().containsKey(index)) {
        taskResult = response.getResults().get(index);
      }
      long containerCompletionTime = 0;
      if (response.getContainerCompletionTime().containsKey(index)) {
        containerCompletionTime = response.getContainerCompletionTime().get(index);
      }
      long taskDuration = 0;
      if (response.getDurations().containsKey(index)) {
        taskDuration = response.getDurations().get(index);
      }
      String task = new StringBuilder(
        response.getJobID() + ", ")
        .append(index + ", ")
        .append(sentTime + ", ")
        .append(response.getAPP_SUBMIT_TIME() + ", ")
        .append(response.getAmStartTime()+ ", ")
        .append(response.getContainerAllocationTimes().get(index) + ", ")
        .append(containerStartTime + ", ")
        .append(receiveTime + ", ")
        .append(taskResult + ", ")
        .append(containerCompletionTime + ", ")
        .append(taskDuration)
        .toString();

      tasks.put(index, task);
    }

    synchronized (server.writerLock) {
      server.jobWriter.write(job);
      server.jobWriter.newLine();

      for (String task : tasks.values()) {
        server.taskWriter.write(task);
        server.taskWriter.newLine();
      }
    }
  }

  @Override
  public String toString() {
    return "YarnResponseWriter[" + response.jobID +"]";
  }
}
