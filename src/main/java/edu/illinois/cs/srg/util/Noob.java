package edu.illinois.cs.srg.util;

import java.lang.management.ManagementFactory;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by gourav on 11/23/14.
 */
public class Noob {

  public static void main(String[] args) {
    /*System.out.println(System.getProperty("user.home"));
    System.out.println(Runtime.getRuntime().availableProcessors());
    System.out.println(ManagementFactory.getThreadMXBean().getThreadCount());

    DecimalFormat format = new DecimalFormat("####.#");
    System.out.println(format.format(334.46454));*/

    /*ExecutorService service = Executors.newFixedThreadPool(3);
    for (int i=0; i<10; i++) {
      service.execute(new Task(i));
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    service.shutdown();*/

    /*String str = "6486624020, 38462127, o/0PSEOQ9jWyzNMXehU3dXqePQIE2QxyxyknZMT1k4c=, , 0, ";
    String parts[] = str.split(", ", -1);
    System.out.print(Arrays.toString(parts));*/

    System.out.println("" + (6000 % 6000));
  }

  static class Task implements Runnable {

    int i;

    Task(int i) {
      this.i = i;
    }

    @Override
    public void run() {
      System.out.println("Running " + i);
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      System.out.println("Terminating " + i);
    }
  }
}
