package org.ruppyrup.reconreplay;

import java.io.Serializable;

/**
 * ReconResults holds a completed ReconUnit and any notifications
 */
public class ReconResult implements Serializable {
  private final ReconUnit reconUnit;
  private final String reconNotification;

  public ReconResult(final ReconUnit reconUnit, final String reconNotification) {
    this.reconUnit = reconUnit;
    this.reconNotification = reconNotification;
  }

  @Override
  public String toString() {
    return "ReconResult{" +
               "reconUnit=" + reconUnit.getEventCount() +
               ", reconNotification='" + reconNotification + '\'' +
               '}';
  }

  public ReconUnit getReconUnit() {
    return reconUnit;
  }
}
