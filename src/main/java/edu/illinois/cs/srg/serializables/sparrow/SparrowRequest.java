package edu.illinois.cs.srg.serializables.sparrow;

import edu.illinois.cs.srg.serializables.AbstractRequest;

import java.io.Serializable;

/**
 * Created by gourav on 2/28/15.
 */
public class SparrowRequest extends AbstractRequest {
  double cpu;
  double mem;

  public SparrowRequest(long jobID, double cpu, double mem) {
    super(jobID, 0);
    this.cpu = cpu;
    this.mem = mem;
  }
}
