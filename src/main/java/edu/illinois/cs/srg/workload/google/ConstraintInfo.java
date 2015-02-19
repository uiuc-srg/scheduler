package edu.illinois.cs.srg.workload.google;

import java.io.Serializable;

/**
 * Created by gourav on 12/3/14.
 */
public class ConstraintInfo implements Serializable {
  String name;
  int operator;
  String value;

  public ConstraintInfo(String name, int operator, String value) {
    this.name = name;
    this.operator = operator;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public int getOperator() {
    return operator;
  }

  public String getValue() {
    return value;
  }
}
