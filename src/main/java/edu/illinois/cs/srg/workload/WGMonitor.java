package edu.illinois.cs.srg.workload;

import edu.illinois.cs.srg.util.Constants;
import edu.illinois.cs.srg.util.Monitor;

import java.io.IOException;

/**
 * Created by gourav on 11/24/14.
 */
public class WGMonitor extends Monitor {


  public WGMonitor(String file) throws IOException {
    super(file);
  }

  @Override
  public boolean terminate() {
    //return false;
    return WorkloadGenerator.terminate;
  }
}