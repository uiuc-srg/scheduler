package edu.illinois.cs.srg.util;

import edu.illinois.cs.srg.workload.WorkloadGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.text.DecimalFormat;

/**
 * Created by gourav on 11/24/14.
 */
public abstract class Monitor implements Runnable {
  protected final Logger LOG = LoggerFactory.getLogger(Monitor.class);

  protected MemoryMXBean memoryMXBean;
  protected ThreadMXBean threadMXBean;
  protected OperatingSystemMXBean operatingSystemMXBean;

  BufferedWriter writer;

  protected DecimalFormat formatter = new DecimalFormat("##.##");

  protected long startTime;

  protected int dyingLimit = 10;

  public Monitor(String file) throws IOException {
    memoryMXBean = ManagementFactory.getMemoryMXBean();
    threadMXBean = ManagementFactory.getThreadMXBean();
    operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();

    writer = new BufferedWriter(new FileWriter(new File(file)));

    startTime = System.currentTimeMillis();

  }

  public abstract boolean terminate();

  public String managementStats() {
    double usage = memoryMXBean.getHeapMemoryUsage().getUsed();
    double memoryStats = 100 * (usage / memoryMXBean.getHeapMemoryUsage().getMax());

    long threadStats = threadMXBean.getThreadCount();
    double load = operatingSystemMXBean.getSystemLoadAverage();
    return (System.currentTimeMillis() - startTime)/1000 + ", " + formatter.format(memoryStats) + ", " + threadStats + ", " + load + "\n";
  }

  @Override
  public void run() {
    int dyingDrama = 0;
    while (dyingDrama < dyingLimit) {
      if (terminate()) {
        dyingDrama++;
      }
      try {
        Thread.sleep(Constants.MONITOR_INTERVAL);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      try {
        writer.write(managementStats());
        writer.newLine();
        writer.flush();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    try {
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
