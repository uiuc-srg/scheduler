package edu.illinois.cs.srg.workload;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import edu.illinois.cs.srg.serializables.ScheduleRequest;
import edu.illinois.cs.srg.serializables.ScheduleResponse;
import edu.illinois.cs.srg.scheduler.TaskInfo;
import edu.illinois.cs.srg.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Map;
import java.util.Set;

/**
 * Created by gourav on 10/17/14.
 */
public class WorkloadGenerator {
  private static final Logger log = LoggerFactory.getLogger(WorkloadGenerator.class);

  String schedulerAddress;

  public WorkloadGenerator(String schedulerAddress) {
    this.schedulerAddress = schedulerAddress;
  }

  public void generate() {
    long id = 1;
    try {
      while (true) {
        Map<Integer, TaskInfo> tasks = Maps.newHashMap();
        tasks.put(0, new TaskInfo(0.25, 0.25, 1000));
        tasks.put(1, new TaskInfo(0.25, 0.25, 1000));
        sendRequest(new ScheduleRequest(id++, tasks));
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public void sendRequest(ScheduleRequest request) throws IOException, ClassNotFoundException {
      Socket clientSocket = new Socket(schedulerAddress, Constants.JOB_SERVER_PORT);
      ObjectOutputStream outStream = new ObjectOutputStream(clientSocket.getOutputStream());
      outStream.flush();
      ObjectInputStream inStream = new ObjectInputStream(clientSocket.getInputStream());
      outStream.writeObject(request);
      ScheduleResponse response = (ScheduleResponse) inStream.readObject();

      log.info("Got Response: {}", response);
      outStream.close();
      inStream.close();
      clientSocket.close();
  }

  public static void main(String args[])  {
    String schedulerAddress = "127.0.0.1";
    if (args.length > 0) {
      schedulerAddress = args[0];
    }
    WorkloadGenerator generator = new WorkloadGenerator(schedulerAddress);
    generator.generate();
  }
}
