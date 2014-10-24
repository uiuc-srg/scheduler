package edu.illinois.cs.srg.cluster;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by gourav on 10/17/14.
 */
public class SimpleClusterState {

    private static final Logger log = LoggerFactory.getLogger(SimpleClusterState.class);
    private static final int DEFAULT_CLUSTER_SIZE = 12000;
    public static Map<Long, Node> nodes;
    public static Map<Long, Usage> usage;


    public static Object lock;

    public Object getLock() {
        return lock;
    }

    //TODO: Make it to where we can specify the number & kind of machines
    public static void init() {
        lock = new Object();
        nodes = new HashMap<Long, Node>();
        usage = Maps.newHashMap();
        // create 12K nodes with 0.5 cpu and memory.
        for (long i = 0; i < DEFAULT_CLUSTER_SIZE; i++) {
            nodes.put(i, new Node(i, 0.5, 0.5));
            usage.put(i, new Usage());
        }
    }

    public SimpleClusterState() {
        this(DEFAULT_CLUSTER_SIZE);
    }

    //TODO: Should this also take a ClusterDescription ? This could be a set of node descriptions to specify kinds of nodes
    public SimpleClusterState(int numMachines) {
        lock = new Object();
        nodes = new HashMap<Long, Node>();
        usage = Maps.newHashMap();

        // create 12K nodes with 0.5 cpu and memory.
        for (long i = 0; i < numMachines; i++) {
            nodes.put(i, new Node(i, 0.5, 0.5));
            usage.put(i, new Usage());
        }
    }

    public Map<Long, Usage> getUsage() {
        return usage;
    }

    //NOTE: Called by remote cluster to update/plsce jobs
    public boolean updateUsage(Map<Long, Usage> usage) {
        //TODO: Add logic here
        return false;
    }
}
