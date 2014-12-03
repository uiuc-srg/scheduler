package edu.illinois.cs.srg.cluster;

import edu.illinois.cs.srg.util.Constants;
import edu.illinois.cs.srg.util.Monitor;
import edu.illinois.cs.srg.workload.WorkloadGenerator;

import java.io.IOException;

/**
 * Created by gourav on 11/24/14.
 */
public class ClusterMonitor extends Monitor {

  public ClusterMonitor(String file) throws IOException {
    super(file);
  }

  @Override
  public boolean terminate() {
    //return false;
    return ClusterEmulator.terminate;
  }
}
