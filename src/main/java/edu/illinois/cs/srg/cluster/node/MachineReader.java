package edu.illinois.cs.srg.cluster.node;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import edu.illinois.cs.srg.util.Constants;
import edu.illinois.cs.srg.workload.google.ConstraintInfo;
import edu.illinois.cs.srg.workload.google.GoogleJob;
import edu.illinois.cs.srg.workload.google.TraceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by gourav on 12/3/14.
 */
public class MachineReader {
  private static final Logger LOG = LoggerFactory.getLogger(MachineReader.class);

  BufferedReader attributeReader;

  public Set<MachineInfo> getMachines(int limit) throws IOException {
    Set<MachineInfo> machines = Sets.newHashSet();

    String file = System.getProperty("user.home") + "/traces/machines";
    BufferedReader reader = new BufferedReader(new FileReader(new File(file)));

    String attributeFile = System.getProperty("user.home") + "/traces/machinesAttributes";
    attributeReader = new BufferedReader(new FileReader(new File(attributeFile)));

    String machineLine;
    while (((machineLine = reader.readLine()) != null) && machines.size() < limit) {
      String[] machine = machineLine.split(", ", -1);
      long id = Long.parseLong(machine[0]);
      double cpu = Double.parseDouble(machine[1]);
      double memory = Double.parseDouble(machine[2]);

      String attributeLine = attributeReader.readLine();
      if (attributeLine == null) {
        LOG.error("Machine - Attribute size mismatch");
        break;
      }
      String[] attribute = attributeLine.split(", ");
      long idFromAttribute = Long.parseLong(attribute[0]);
      if (idFromAttribute != id) {
        LOG.error("Machine - Attribute size mismatch");
        return null;
      }
      Map<String, String> attributeMap = Maps.newHashMap();
      for (int i=1; i<attribute.length; i=i+2) {
        attributeMap.put(attribute[i], attribute[i+1]);
      }
      MachineInfo machineInfo = new MachineInfo(id, cpu, memory, attributeMap);
      machines.add(machineInfo);
    }
    reader.close();

    return machines;
  }

  public static class MachineInfo {
    public long id;
    public double cpu;
    public double memory;
    public Map<String, String> attributes;

    MachineInfo(long id, double cpu, double memory, Map<String, String> attributes) {
      this.id = id;
      this.cpu = cpu;
      this.memory = memory;
      this.attributes = attributes;
    }
  }

  private static boolean match(ConstraintInfo con, MachineInfo machine) {
    if (con.getOperator() == Constants.CONS_EQUAL) {
      String supply = (machine.attributes.containsKey(con.getName())) ? machine.attributes.get(con.getName()) : "";
      String demand = con.getValue();
      return demand.equals(supply);
    } else if (con.getOperator() == Constants.CONS_NOT_EQUAL) {
      String supply = (machine.attributes.containsKey(con.getName())) ? machine.attributes.get(con.getName()) : "";
      String demand = con.getValue();
      return !demand.equals(supply);
    } else if (con.getOperator() == Constants.CONS_LESSER) {
      int supply = (machine.attributes.containsKey(con.getName())) ? Integer.parseInt(machine.attributes.get(con.getName())) : 0;
      int demand = Integer.parseInt(con.getValue());
      return (supply < demand);
    } else if (con.getOperator() == Constants.CONS_GREATER) {
      int supply = (machine.attributes.containsKey(con.getName())) ? Integer.parseInt(machine.attributes.get(con.getName())) : 0;
      int demand = Integer.parseInt(con.getValue());
      return (supply > demand);
    }
    LOG.error("Unidentified Operator - {}", con.getOperator());
    return false;
  }

  private static boolean match(Set<ConstraintInfo> cons, Set<MachineInfo> machines) {

    boolean foundAMachine = false;
    for (MachineInfo machine : machines) {
      boolean success = true;
      for (ConstraintInfo con : cons) {
        if (!match(con, machine)) {
          success = false;
          break;
        }
      }

      if (success) {
        foundAMachine = true;
        break;
      }
    }
    return foundAMachine;
  }

  public static void main(String[] args) {
    long unmatchedTasks = 0;
    MachineReader reader = new MachineReader();
    try {
      Set<MachineInfo> machines = reader.getMachines(6000);

      TraceReader traceReader = new TraceReader(System.getProperty("user.home") + "/traces/attributes", System.getProperty("user.home") + "/traces/durationsNoNaN", System.getProperty("user.home") + "/traces/constraints");
      List<GoogleJob> jobs =  traceReader.getJobs();

      // lets check if all cons task have at least one matching job.
      for (GoogleJob googleJob : jobs) {
        for (Set<ConstraintInfo> cons : googleJob.getCons()) {
          if (!match(cons, machines)) {
            unmatchedTasks++;
          }
        }
      }
      LOG.info("Unmatched Tasks {}", unmatchedTasks);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
