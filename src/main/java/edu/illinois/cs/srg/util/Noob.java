package edu.illinois.cs.srg.util;

import java.lang.management.ManagementFactory;
import java.text.DecimalFormat;

/**
 * Created by gourav on 11/23/14.
 */
public class Noob {

  public static void main(String[] args) {
    System.out.println(System.getProperty("user.home"));
    System.out.println(Runtime.getRuntime().availableProcessors());
    System.out.println(ManagementFactory.getThreadMXBean().getThreadCount());

    DecimalFormat format = new DecimalFormat("####.#");
    System.out.println(format.format(334.46454));
  }
}
