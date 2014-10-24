package edu.illinois.cs.srg.scheduler;

import java.io.Serializable;

/**
 * Created by gourav on 10/17/14.
 */
public class ScheduleResponse implements Serializable {

  public static final int SUCCESS = 1;
  public static final int FAIL = 0;

  int result;

  public ScheduleResponse(int result) {
    this.result = result;
  }

  public ScheduleResponse(boolean result) {
      if (result)
          this.result = 1;
      else
          this.result = 0;
  }
}
