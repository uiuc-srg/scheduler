package edu.illinois.cs.srg.workload.yarn;

import edu.illinois.cs.srg.scheduler.Debugger;
import edu.illinois.cs.srg.util.Constants;
import edu.illinois.cs.srg.util.Monitor;
import edu.illinois.cs.srg.workload.WorkloadGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.*;

/**
 * Created by gourav on 11/24/14.
 */
public class YarnMonitor extends Monitor {
  private static EnumSet<YarnApplicationState> RUNNING_APPS = EnumSet.of(YarnApplicationState.RUNNING);
  private static EnumSet<YarnApplicationState> FAILED_APPS = EnumSet.of(YarnApplicationState.KILLED, YarnApplicationState.FAILED);

  private Configuration conf;
  private YarnClient yarnClient;
  private String rmAddress;

  public YarnMonitor(String file, String rmAddress) throws IOException {
    super(file);

    RUNNING_APPS.add(YarnApplicationState.RUNNING);
    FAILED_APPS.add(YarnApplicationState.FAILED);
    FAILED_APPS.add(YarnApplicationState.KILLED);

    this.rmAddress = rmAddress;
    conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_ADDRESS, rmAddress + ":8032");
    conf.set(YarnConfiguration.RM_WEBAPP_DELEGATION_TOKEN_AUTH_FILTER, "false");
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();
  }

  @Override
  public String managementStats() {
    long threadStats = threadMXBean.getThreadCount();
    int memoryUsed = 0;
    int cpuUsed = 0;
    int memoryTotal = 0;
    int cpuTotal = 0;
    double memoryUtil = -1;
    double cpuUtil = -1;
    double cpuVariance = -1;
    double memoryVariance = -1;
    int numRunningApps = 0;
    int numFailedApps = 0;
    try {

      List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(NodeState.RUNNING, NodeState.NEW);
      List<ApplicationReport> runningApps = yarnClient.getApplications(RUNNING_APPS);
      List<ApplicationReport> failedApps = yarnClient.getApplications(FAILED_APPS);

      double[] cpuDist = new double[clusterNodeReports.size()];
      double[] memDist = new double[clusterNodeReports.size()];
      int i=0;
      for (NodeReport node : clusterNodeReports) {
        memoryTotal += node.getCapability().getMemory();
        cpuTotal += node.getCapability().getVirtualCores();

        memoryUsed += node.getUsed().getMemory();
        cpuUsed += node.getUsed().getVirtualCores();

        cpuDist[i] = node.getUsed().getVirtualCores()*1.0 / node.getCapability().getVirtualCores()*1.0;
        memDist[i] = node.getUsed().getMemory()*1.0 / node.getCapability().getMemory()*1.0;
        i++;
      }
      cpuVariance = getVariance(cpuDist);
      memoryVariance = getVariance(memDist);

      numRunningApps = runningApps.size();
      numFailedApps = failedApps.size();

    } catch (YarnException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ArithmeticException e) {
      e.printStackTrace();
    }
    if (memoryTotal != 0) {
      memoryUtil = memoryUsed*1.0 / memoryTotal*1.0;
    }
    if (cpuTotal != 0) {
      cpuUtil = cpuUsed*1.0 / cpuTotal*1.0;
    }
    // (1)time
    // (2)cpuUtil (3)memoryUtil (4)cpuVariance (5)memoryVariance
    // (6)cpuTotal (7)memTotal (8)cpuUsage (9)memUsage
    // (10)runningApps (11)failedApps (12)thread
    return (System.currentTimeMillis() - startTime)/1000 + ", " +
      formatter.format(cpuUtil) + ", " + formatter.format(memoryUtil)  + ", " + formatter.format(cpuVariance) + ", " + formatter.format(memoryVariance) + ", " +
      cpuTotal + ", " + memoryTotal + ", " + cpuUsed + ", " + memoryUsed + ", " +
      numRunningApps + ", " + numFailedApps + ", " + threadStats + "\n";
  }

  @Override
  public boolean terminate() {
    //return false;
    return WorkloadGenerator.terminate;
  }

  double getVariance(double[] data) {
    if (data.length == 0) {
      return 0;
    }
    double mean = getMean(data);
    double temp = 0;
    for(double a :data) {
      temp += (mean - a) * (mean - a);
    }
    return Math.sqrt(temp/data.length);
  }

  double getMean(double[] data) {
    double sum = 0.0;
    for(double a : data)
      sum += a;
    return sum/data.length;
  }

}
