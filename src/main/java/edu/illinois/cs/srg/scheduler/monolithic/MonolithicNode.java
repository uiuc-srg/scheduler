package edu.illinois.cs.srg.scheduler.monolithic;

import edu.illinois.cs.srg.scheduler.AbstractJobHandler;
import edu.illinois.cs.srg.scheduler.Node;
import edu.illinois.cs.srg.serializables.AbstractRequest;
import edu.illinois.cs.srg.serializables.Heartbeat;
import edu.illinois.cs.srg.serializables.monolithic.PlacementRequest;
import edu.illinois.cs.srg.serializables.PlacementResponse;
import edu.illinois.cs.srg.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;

/**
 * Created by gourav on 2/24/15.
 */
public class MonolithicNode extends Node {
  private static final Logger LOG = LoggerFactory.getLogger(MonolithicNode.class);

  public MonolithicNode(Socket socket) throws IOException, ClassNotFoundException {
    super(socket);
  }

  @Override
  public boolean update() throws IOException {
    try {
      Object object = input.readObject();
      try {
        Heartbeat heartbeat = (Heartbeat) object;
        //LOG.debug("{} for {}", heartbeat, this);
        synchronized (resourceLock) {
          this.availableCPU = heartbeat.availableCPU;
          this.availableMemory = heartbeat.availableMemory;
        }
      } catch (ClassCastException e) {
        PlacementResponse placementResponse = (PlacementResponse) object;
        placementResponse.setRecvSchedulerCluster(System.currentTimeMillis());
        //LOG.debug("{} for {}", placementResponse, this);

        if (placementResponse.getJobID() == Constants.SIGTERM) {
          //LOG.debug("{}: Got SIGTERM", this);
          if (pendingRequests.size() > 0) {
            LOG.warn("{} shutting down with some requests pending.", this);
          }
          return true;
        }

        // no lock required here

        boolean success = false;

        while (pendingRequests.size() > 0) {
          RequestInfo requestInfo = pendingRequests.poll();
          placementResponse.setSentSchedulerCluster(requestInfo.sentSchedulerCluster);
          if (requestInfo.request.getJobID() == placementResponse.getJobID() &&
            requestInfo.request.getIndex() == placementResponse.getIndex()) {

            synchronized (requestInfo.jobHandler) {
              if (requestInfo.jobHandler.shouldIKnock()) {
                requestInfo.jobHandler.addResponse(placementResponse);
                requestInfo.jobHandler.notify();
              } else {
                LOG.error("{}: JobHandler do not want the response no more", this);
              }
            }
            success = true;
            break;
          } else {
            // request should have matched.
            LOG.error("ERROR 2");
          }
        }

        if (!success) {
          // there should have been a request.
          LOG.error("ERROR 1", placementResponse);
        }
      }
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (NullPointerException e) {
      e.printStackTrace();
    }
    return false;
  }

  @Override
  public void schedule(AbstractJobHandler jobHandler, AbstractRequest request) throws IOException {
    synchronized (requestLock) {
      if (request.getJobID() != Constants.SIGTERM) {
        pendingRequests.add(new RequestInfo(jobHandler, request, System.currentTimeMillis()));
      }
      this.output.writeObject(request);
      this.output.flush();
    }
  }
}
