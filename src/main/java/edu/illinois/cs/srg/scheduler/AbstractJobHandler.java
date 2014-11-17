package edu.illinois.cs.srg.scheduler;

import edu.illinois.cs.srg.serializables.ScheduleRequest;
import edu.illinois.cs.srg.serializables.ScheduleResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Created by read on 10/24/14.
 */
public abstract class AbstractJobHandler implements Runnable {

  protected static final Logger log = LoggerFactory.getLogger(AbstractJobHandler.class);

  ScheduleRequest request;
  Socket socket;

  public AbstractJobHandler(Socket socket) {
    this.socket = socket;
  }

  public abstract ScheduleResponse schedule(ScheduleRequest scheduleRequest);

  @Override
  public void run() {
    try {
      ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
      ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
      request = (ScheduleRequest) inputStream.readObject();
      log.info("Received: " + request);

      ScheduleResponse response = schedule(request);

      outputStream.writeObject(response);

      // TODO: how the task will end ?
      inputStream.close();
      outputStream.close();
      socket.close();
    } catch (Exception e) {
      log.error("Discarding request {}", request);
      e.printStackTrace();
    }
  }
}
