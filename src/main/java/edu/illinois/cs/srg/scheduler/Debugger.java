package edu.illinois.cs.srg.scheduler;

/**
 * Created by gourav on 11/27/14.
 */
public class Debugger {

  public static int totalNodes = 0;
  public static int totalRequests = 0;
  private static Object lock = new Object();

  public static void increment() {
    synchronized (lock) {
      totalNodes++;
    }
  }

  public static void decrement() {
    synchronized (lock) {
      totalNodes--;
    }
  }

  public static void addRequest() {
    synchronized (lock) {
      totalRequests++;
    }
  }

}
