package edu.illinois.cs.srg.scheduler;

import edu.illinois.cs.srg.util.Constants;
import edu.illinois.cs.srg.util.Monitor;
import edu.illinois.cs.srg.workload.WorkloadGenerator;

import java.io.IOException;

/**
 * Created by gourav on 11/24/14.
 */
public class SchedulerMonitor extends Monitor {

  public SchedulerMonitor(String file) throws IOException {
    super(file);
  }

  @Override
  public String managementStats() {
    double usage = memoryMXBean.getHeapMemoryUsage().getUsed();
    double memoryStats = 100 * (usage / memoryMXBean.getHeapMemoryUsage().getMax());
    long threadStats = threadMXBean.getThreadCount();
    double load = operatingSystemMXBean.getSystemLoadAverage();
    return (System.currentTimeMillis() - startTime)/1000 + ", " + formatter.format(memoryStats) + ", " + threadStats + ", " + load + ", " + Debugger.totalNodes + ", " + Debugger.totalRequests + "\n";
  }

  @Override
  public boolean terminate() {
    //return false;
    return Scheduler.terminate;
  }
}
