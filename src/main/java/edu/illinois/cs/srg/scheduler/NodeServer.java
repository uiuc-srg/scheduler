package edu.illinois.cs.srg.scheduler;

import edu.illinois.cs.srg.cluster.node.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by gourav on 11/15/14.
 */
public class NodeServer implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(NodeServer.class);

  ClusterState clusterState;

  public NodeServer(ClusterState clusterState) {
    this.clusterState = clusterState;
  }

  @Override
  public void run() {
    // This will receive new node requests.
    try {
      ServerSocket serverSocket = new ServerSocket(Constants.NODE_SERVER_PORT);

      while (true) {
        Socket socket = serverSocket.accept();
        try {
          clusterState.add(new Node(socket));
        } catch (ClassNotFoundException e) {
          e.printStackTrace();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
