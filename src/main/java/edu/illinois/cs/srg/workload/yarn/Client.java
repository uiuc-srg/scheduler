package edu.illinois.cs.srg.workload.yarn;

import com.google.common.collect.Maps;
import edu.illinois.cs.srg.util.Constants;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.yarn.api.records.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.util.*;


@InterfaceAudience.Public
@InterfaceStability.Unstable
public class Client implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Client.class);

  private final String appMasterMainClass = "edu.illinois.cs.srg.ApplicationMaster";
  private final String jarsPath = "/tmp/jars/*";

  private String appName = "ExpApp";
  private int amPriority = 0;
  private String amQueue = "default";
  private int amMemory = 1024;
  private int amVCores = 0;
  private boolean verbose = false;
  private boolean keepContainers = false;
  public long clientTimeout = 600000;

  private Configuration conf;
  private YarnClient yarnClient;
  private long requestSentTime;
  ApplicationReport report;

  private String rmAddress = "192.17.176.12";
  private String myAddress;
  // Not using mustangNM
  private String mustangNM = "192.17.176.12";
  private long jobID;
  private int ntasks;
  private double cpu;
  private double memory;
  Map<Integer, Long> durations;


  public static void main(String[] args) {
    Map<Integer, Long> testDurations = Maps.newHashMap();
    testDurations.put(0, new Long(1000));
    Client client = new Client(args[0], args[1], "192.17.176.14", 1, 1, 0.5, 0.5, testDurations);
    client.clientTimeout = Long.parseLong(args[2]);
    client.run();
  }

  public Client(String rmAddress, String myAddress, String mustangNM, long jobID, int ntasks, double cpu, double memory, Map<Integer, Long> durations) {
    this.rmAddress = rmAddress;
    this.myAddress = myAddress;
    this.mustangNM = mustangNM;
    this.jobID = jobID;
    this.ntasks = ntasks;
    this.cpu = cpu;
    this.memory = memory;
    this.durations = durations;

    conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_ADDRESS, rmAddress + ":8032");
    conf.set(YarnConfiguration.RM_WEBAPP_DELEGATION_TOKEN_AUTH_FILTER, "false");
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
  }

  @Override
  public void run() {
    try {
      yarnClient.start();
      YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
      if (verbose) {
        LOG.info("Got Cluster metric info from ASM"
          + ", numNodeManagers=" + clusterMetrics.getNumNodeManagers());

        List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(
          NodeState.RUNNING);
        for (NodeReport node : clusterNodeReports) {
          LOG.info("Got node report from ASM for"
            + ", nodeId=" + node.getNodeId()
            + ", nodeAddress" + node.getHttpAddress()
            + ", nodeRackName" + node.getRackName()
            + ", nodeNumContainers" + node.getNumContainers());
        }

        QueueInfo queueInfo = yarnClient.getQueueInfo(this.amQueue);
        LOG.info("Queue info"
          + ", queueName=" + queueInfo.getQueueName()
          + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
          + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
          + ", queueApplicationCount=" + queueInfo.getApplications().size()
          + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());

        List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
        for (QueueUserACLInfo aclInfo : listAclInfo) {
          for (QueueACL userAcl : aclInfo.getUserAcls()) {
            LOG.info("User ACL Info for Queue"
              + ", queueName=" + aclInfo.getQueueName()
              + ", userAcl=" + userAcl.name());
          }
        }
      }

      // Get a new application id
      YarnClientApplication app = yarnClient.createApplication();
      GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

      //Increase memory according to number of tasks
      amMemory = 1024 + 10*ntasks;
      int maxMem = appResponse.getMaximumResourceCapability().getMemory();
      if (amMemory > maxMem) {
        LOG.debug("AM memory specified above max threshold of cluster. Using max value."
          + ", specified=" + amMemory
          + ", max=" + maxMem);
        amMemory = maxMem;
      }
      int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
      if (amVCores > maxVCores) {
        LOG.debug("AM virtual cores specified above max threshold of cluster. "
          + "Using max value." + ", specified=" + amVCores
          + ", max=" + maxVCores);
        amVCores = maxVCores;
      }

      // set the application name
      ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
      ApplicationId appId = appContext.getApplicationId();

      appContext.setKeepContainersAcrossApplicationAttempts(keepContainers);
      appContext.setApplicationName(appName);

      Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
      //LOG.info("NOT Copy App Master jar from local filesystem and add to local environment");
      FileSystem fs = FileSystem.get(conf);
      //addToLocalResources(fs, appMasterJar, appMasterJarPath, appId.toString(), localResources, null);

      // Set the env variables to be setup in the env where the application master will be run
      //LOG.info("Set the environment for the application master");
      Map<String, String> env = new HashMap<String, String>();
      // Add AppMaster.jar location to classpath
      // At some point we should not be required to add
      // the hadoop specific classpaths to the env.
      // It should be provided out of the box.
      // For now setting all required classpaths including
      // the classpath to "." for the application jar
      StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
        .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
      for (String c : conf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
        classPathEnv.append(c.trim());
      }
      classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
      classPathEnv.append(jarsPath);
      env.put("CLASSPATH", classPathEnv.toString());

      // command
      Vector<CharSequence> vargs = new Vector<CharSequence>(30);
      vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
      vargs.add("-Xmx" + amMemory + "m");
      vargs.add(appMasterMainClass);
      vargs.add(rmAddress);
      vargs.add(myAddress);
      vargs.add(jobID + "");
      vargs.add(cpu + "");
      vargs.add(memory + "");
      vargs.add(ntasks + "");
      // Sort request according in Decreasing order.
      List<Long> sortedDurations = new ArrayList<Long>(durations.values());
      Collections.sort(sortedDurations, new Comparator<Long>() {
        @Override
        public int compare(Long o1, Long o2) {
          return -1*Long.compare(o1, o2);
        }
      });
      for (long duration : sortedDurations) {
        vargs.add(duration + "");
        if (duration > clientTimeout) {
          clientTimeout = duration;
        }
      }

      vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
      vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");
      StringBuilder command = new StringBuilder();
      for (CharSequence str : vargs) {
        command.append(str).append(" ");
      }
      LOG.debug("AM Command " + command.toString());
      List<String> commands = new ArrayList<String>();
      commands.add(command.toString());

      // Set up the container launch context for the application master
      ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
        localResources, env, commands, null, null, null);

      Resource capability = Resource.newInstance(amMemory, amVCores);
      appContext.setResource(capability);
      //ResourceRequest resourceRequest = ResourceRequest.newInstance(Priority.newInstance(amPriority), mustangNM, capability, 1, false);
      //appContext.setAMContainerResourceRequest(resourceRequest);
      appContext.setAMContainerSpec(amContainer);
      Priority pri = Priority.newInstance(amPriority);
      appContext.setPriority(pri);
      appContext.setQueue(amQueue);

      requestSentTime = System.currentTimeMillis();
      LOG.debug("Submitting application to ASM");
      YarnResponseServer.waitingJobs.put(jobID, requestSentTime);
      yarnClient.submitApplication(appContext);


      //TODO: should we monitor ?
      if (monitorApplication(appId)) {
        LOG.debug("App {} ran successfully", appId);
      } else {
        YarnResponseServer.waitingJobs.remove(jobID);
        LOG.error("App {} Failed.", appId);
        if (report != null) {
          report = yarnClient.getApplicationReport(appId);
          LOG.info("Got application report from ASM for"
            + ", appId=" + appId.getId()
            + ", appDiagnostics=" + report.getDiagnostics()
            + ", appMasterHost=" + report.getHost()
            + ", appQueue=" + report.getQueue()
            + ", appMasterRpcPort=" + report.getRpcPort()
            + ", appStartTime=" + report.getStartTime()
            + ", yarnAppState=" + report.getYarnApplicationState().toString()
            + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
            //+ ", appTrackingUrl=" + report.getTrackingUrl()
            + ", appUser=" + report.getUser());
        }
      }

    } catch (IOException e) {
      e.printStackTrace();
    } catch (YarnException e) {
      e.printStackTrace();
    }
  }

  /**
   * Monitor the submitted application for completion.
   * Kill application if time expires.
   * @param appId Application Id of application to be monitored
   * @return true if application completed successfully
   * @throws YarnException
   * @throws IOException
   */
  private boolean monitorApplication(ApplicationId appId)
    throws YarnException, IOException {

    LOG.debug("App {} timeout - {}", appId, clientTimeout);
    long startTime = System.currentTimeMillis();
    while (true) {

      try {
        Thread.sleep(clientTimeout / 3);
      } catch (InterruptedException e) {
        LOG.debug("Thread sleep in monitoring loop interrupted");
      }

      // Get application report for the appId we are interested in
      report = yarnClient.getApplicationReport(appId);

      LOG.debug("Got application report from ASM for"
        + ", appId=" + appId.getId()
        + ", appDiagnostics=" + report.getDiagnostics()
        + ", appMasterHost=" + report.getHost()
        + ", appQueue=" + report.getQueue()
        + ", appMasterRpcPort=" + report.getRpcPort()
        + ", appStartTime=" + report.getStartTime()
        + ", yarnAppState=" + report.getYarnApplicationState().toString()
        + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
        //+ ", appTrackingUrl=" + report.getTrackingUrl()
        + ", appUser=" + report.getUser());

      YarnApplicationState state = report.getYarnApplicationState();
      FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
      if (YarnApplicationState.FINISHED == state) {
        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
          //LOG.info("Application has completed successfully.");
          return true;
        }
        else {
          //LOG.info("Application did finished unsuccessfully."
          //  + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString());
          return false;
        }
      }
      else if (YarnApplicationState.KILLED == state
        || YarnApplicationState.FAILED == state) {
        //LOG.info("Application did not finish."
         // + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString());
        return false;
      }

      if (System.currentTimeMillis() > (startTime + clientTimeout + 10*Constants.TIMEOUT)) {
        LOG.warn("Reached client specified timeout {} seconds. I waited for {} seconds. Killing application",
          (clientTimeout + 10*Constants.TIMEOUT) / 1000, (System.currentTimeMillis() - startTime)/1000);
        forceKillApplication(appId);
        return false;
      }
    }
  }

  /**
   * Kill a submitted application by sending a call to the ASM
   * @param appId Application Id to be killed.
   * @throws YarnException
   * @throws IOException
   */
  private void forceKillApplication(ApplicationId appId) throws YarnException, IOException {
    yarnClient.killApplication(appId);
  }

  private void addToLocalResources(FileSystem fs, String fileSrcPath,
                                   String fileDstPath, String appId, Map<String, LocalResource> localResources,
                                   String resources) throws IOException {
    String suffix =
      //appName + "/" + appId + "/" + fileDstPath;
      appName + "/" + fileDstPath;
    Path dst = new Path(fs.getHomeDirectory(), suffix);
    //Path macPath = new Path("/Users/gourav/code/scheduler/target/scheduler-1.0-SNAPSHOT.jar");
    //Path nodeManagerPath = new Path("file:///home/scheduler/AppMaster.jar");
    if (fileSrcPath == null) {
      FSDataOutputStream ostream = null;
      try {
        ostream = FileSystem
          .create(fs, dst, new FsPermission((short) 0710));
        ostream.writeUTF(resources);
      } finally {
        IOUtils.closeQuietly(ostream);
      }
    } else {
      fs.copyFromLocalFile(new Path(fileSrcPath), dst);
    }
    FileStatus scFileStatus = fs.getFileStatus(dst);
    //FileStatus scFileStatus = fs.getFileStatus(macPath);

    LocalResource scRsrc =
      LocalResource.newInstance(
        ConverterUtils.getYarnUrlFromURI(dst.toUri()),
        //ConverterUtils.getYarnUrlFromURI(nodeManagerPath.toUri()),
        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
        scFileStatus.getLen(), scFileStatus.getModificationTime());

    localResources.put(fileDstPath, scRsrc);
  }


}
