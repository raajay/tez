package org.apache.tez.dag.app.dag.impl;

public class PendingDagEventProcessor implements Runnable {

  private ClockedScheduler _dag_scheduler;

  public PendingDagEventProcessor (ClockedScheduler scheduler) {
    this._dag_scheduler = scheduler;
  }

  @Override
  public void run() {
    _dag_scheduler.clearPendingEvents();
  }

}
