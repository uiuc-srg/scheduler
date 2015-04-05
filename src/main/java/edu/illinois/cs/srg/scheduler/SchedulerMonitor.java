package edu.illinois.cs.srg.scheduler;

import edu.illinois.cs.srg.util.Constants;
import edu.illinois.cs.srg.util.Monitor;
import edu.illinois.cs.srg.workload.WorkloadGenerator;

import java.io.IOException;

/**
 * Created by gourav on 11/24/14.
 */
public class SchedulerMonitor extends Monitor {

  static long timeouts;
  static Object timeoutLock;
  static long failedAttempts;
  static Object attemptLock;

  public SchedulerMonitor(String file) throws IOException {
    super(file);
    timeouts = 0;
    timeoutLock = new Object();
    failedAttempts = 0;
    attemptLock = new Object();
  }

  public static void incrementTimeouts() {
    synchronized (timeoutLock) {
      timeouts++;
    }
  }

  public static void incrementAttempts() {
    synchronized (attemptLock) {
      failedAttempts++;
    }
  }

  @Override
  public String managementStats() {
    double usage = memoryMXBean.getHeapMemoryUsage().getUsed();
    double memoryStats = 100 * (usage / memoryMXBean.getHeapMemoryUsage().getMax());
    long threadStats = threadMXBean.getThreadCount();
    double load = operatingSystemMXBean.getSystemLoadAverage();
    //(1)time (2)heap_memory_percent (3)#threads (4)load (5)total_nodes (6)total_requests (7)timeouts (8)failed_requests
    return (System.currentTimeMillis() - startTime)/1000 + ", " + formatter.format(memoryStats) + ", " + threadStats + ", " + load + ", " + Debugger.totalNodes + ", " + Debugger.totalRequests + ", " + timeouts + ", " + failedAttempts + "\n";
  }

  @Override
  public boolean terminate() {
    //return false;
    return Scheduler.terminate;
  }
}
