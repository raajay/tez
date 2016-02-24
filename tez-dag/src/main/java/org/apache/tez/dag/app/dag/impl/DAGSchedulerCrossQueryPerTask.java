/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.app.dag.impl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.DAGScheduler;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdateTAAssigned;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventSchedule;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

/**
 * Schedules task attempts belonging to downstream vertices only after all attempts belonging to
 * upstream vertices have been scheduled. If there's a slow start or delayed start of a particular
 * vertex, this ensures that downstream tasks are not started before this</p>
 * Some future enhancements
 * - consider cluster capacity - and be more aggressive about scheduling downstream tasks before
 * upstream tasks have completed. </p>
 * - generic slow start mechanism across all vertices - independent of the type of edges.
 */
@SuppressWarnings("rawtypes")
public class DAGSchedulerCrossQueryPerTask implements DAGScheduler, ClockedScheduler {

  private static final Logger LOG =
      LoggerFactory.getLogger(DAGSchedulerCrossQueryPerTask.class);

  private static final String SCHEDULE_FOLDER = "/media/raajay/code-netopt/";

  private final DAG dag;
  private final EventHandler handler;

  private final HashMap<String, String> stage2vertex = new HashMap<>();
  private final HashMap<String, HashSet<String>> vertex2stage = new HashMap<>();

  // Tracks pending events, in case they're not sent immediately.
  private final ListMultimap<String, TaskAttemptEventSchedule> pendingEvents =
      LinkedListMultimap.create();

  private final ListMultimap<String, TaskAttemptEventSchedule> subStagePendingEvents =
      LinkedListMultimap.create();

  // Tracks vertices for which no additional scheduling checks are required. Once in this list, the
  // vertex is considered to be fully scheduled.
  private final Set<String> scheduledVertices = new HashSet<String>();
  private final Set<String> scheduledSubStages = new HashSet<>();
  private final HashMap<String, Integer> vertexResponses = new HashMap<>();

  // Track the completed vertices, for which the event has been received
  private final Set<String> completedVertices = new HashSet<String>();

  // Tracks the tasks scheduled for vertices. That is the tasks for which the
  // schedule event has been already sent
  private final Map<String, BitSet> vertexScheduledTasks = new HashMap<String, BitSet>();
  private final Set<String> subStagesWithStartTimes = new HashSet<>();
  // Tracks vertex schedule time relative to first vertex dagStartTime. Is
  // populated upon construction of the object
  private Map<String, Long> scheduleTimes;
  private Long dagStartTime = -1L;
  private ScheduledThreadPoolExecutor _executor;
  private PendingDagEventProcessor _event_processor;
  private Map<String, Boolean> _ordering_constraint_satisfied;

  /**
   * @param dag The dag for which the scheduler is attached
   * @param dispatcher The dispatches who sends events
   */
  public DAGSchedulerCrossQueryPerTask(DAG dag, EventHandler dispatcher) {

    this.dag = dag;
    this.handler = dispatcher;

    init();
    read(SCHEDULE_FOLDER + dag.getName());

    this._event_processor = new PendingDagEventProcessor(this);
    this._executor = new ScheduledThreadPoolExecutor(1);
    // A thread to ping every second for releasing pending events.
    this._executor.scheduleAtFixedRate(_event_processor, 1, 1, TimeUnit.SECONDS);
  }

  private void init() {
    this.scheduleTimes = new HashMap<>();
    this._ordering_constraint_satisfied = new HashMap<>();
    for(Vertex vertex : dag.getVertices().values()) {
      _ordering_constraint_satisfied.put(vertex.getName(), false);
    }
  }

  private void read(String schedule_file) {
    try {
      BufferedReader reader = new BufferedReader(new FileReader(schedule_file));
      String vertexTimePair = null;
      while ((vertexTimePair = reader.readLine()) != null) {
        String[] vt = vertexTimePair.split(":");
        if(vt.length != 2) {
          StringBuilder sb = new StringBuilder();
          sb.append("CQ: Badly formatted line in start time file.");
          sb.append("File : " + schedule_file);
          sb.append("Line : " + vertexTimePair);
          LOG.error(sb.toString());
          continue;
        }
        scheduleTimes.put(vt[0], Long.parseLong(vt[1]));
      }
      reader.close();
    } catch (IOException e) {
      LOG.error("CQ: Reading the schedule file failed. File = " + schedule_file +
          "\n" + e.getMessage());
    }
  }

  @Override
  public void vertexCompleted(Vertex vertex) {
    this.completedVertices.add(vertex.getName());
  }

  // TODO Does ordering matter - it currently depends on the order returned by vertex.getOutput*
  @Override
  public void scheduleTask(DAGEventSchedulerUpdate event) {
    gateway(event);
  }

  /**
   * Update the set of tasks for which "request" has arrived for each vertex.
   * A vertex with no "request" will never come here, unless forced with a null
   * value, just to make its BitSet as zero.
   *
   * @param vertexName The vertex of interest
   * @param taskAttemptID  The task for which requests was made
   */
  private void taskAttemptSeen(String vertexName,
      TezTaskAttemptID taskAttemptID) {

    BitSet scheduledTasks = vertexScheduledTasks.get(vertexName);

    if (scheduledTasks == null) {
      scheduledTasks = new BitSet();
      vertexScheduledTasks.put(vertexName, scheduledTasks);
    }

    if (taskAttemptID != null) { // null for 0 task vertices
      scheduledTasks.set(taskAttemptID.getTaskID().getId());
    }

  }

  private int sendEventsForSubStage(String subStageId) {
    int counter = 0;
    for(TaskAttemptEventSchedule event : subStagePendingEvents.removeAll(subStageId)) {
      sendEvent(event);
      counter++;
    }
    return counter;
  }


  /**
   * Process the specified vertex, and add it to the cache of scheduled vertices if it can be scheduled
   * @param vertex The stage of processing in a DAG
   * @return True/False indicating if vertex can be scheduled (i.e. all
   * conditions are satisfied)
   */
  private boolean trySchedulingVertex(Vertex vertex) {

    boolean canSchedule = true;

    if (vertexScheduledTasks.get(vertex.getName()) == null) {
      // 1.  No scheduled requests seen yet. Do not mark this as ready.
      // 0 task vertices handled elsewhere. DO NOT SCHEDULE
      LOG.debug("No schedule requests for vertex: " +
          vertex.getLogIdentifier() + ", Not scheduling");
      canSchedule = false;

    } else {

      Map<Vertex, Edge> inputVertexEdgeMap = vertex.getInputVertices();

      if (inputVertexEdgeMap == null || inputVertexEdgeMap.isEmpty()) {

        LOG.debug("Encountered a MAP vertex. Name = " + vertex.getName()
            + ". Ordering is satisfied by default. ");

      } else {
        // Check if all sources are scheduled.
        for (Vertex srcVertex : inputVertexEdgeMap.keySet()) {

          if (scheduledVertices.contains(vertex.getName())) {
            // 3. This source vertex has already been scheduled. An request
            // for tasks belonging to this vertex will be responded affirmatively.
            LOG.debug("Parent " + srcVertex.getName() + " is already scheduled.");
          } else {

            // Special case for vertices with 0 tasks. 0 check is sufficient since parallelism cannot increase.
            if (srcVertex.getTotalTasks() == 0) {
              // 4. If parent has zero vertices, it will not satisfy (3)
              LOG.info("Vertex: " + srcVertex.getLogIdentifier() + " has 0 tasks. Marking as scheduled");
              // this is adding only the parent
              scheduledVertices.add(srcVertex.getName());
              taskAttemptSeen(srcVertex.getName(), null);

            } else {
              LOG.debug("Parent " + srcVertex.getName() + " is not scheduled.");
              canSchedule = false;
              break;
            }
          }
        } // end for -- over source vertices
      } // end if else - Current vertex is not a map vertex
    } // end if - at least one request for a task belonging to this vertex has been received

    // Update if ordering constraint has been satisfied
    _ordering_constraint_satisfied.put(vertex.getName(), canSchedule);
    return canSchedule;
  }


  @Override
  public void taskScheduled(DAGEventSchedulerUpdateTAAssigned event) {
  }


  @Override
  public void taskSucceeded(DAGEventSchedulerUpdate event) {
  }


  @SuppressWarnings("unchecked")
  private void sendEvent(TaskAttemptEventSchedule event) {
    handler.handle(event);
  }


  /**
   * {@inheritDoc}
   * @see ClockedScheduler#clearPendingEvents()
   */
  public void clearPendingEvents() {
    gateway(null);
  }

  /**
   * The gateway function to do any kind of processing to internal state of the
   * scheduler. We synchronize it because we want to avoid race condition
   * between the clock thread and the main AM thread.
   * @param event Can be null.
   */
  private synchronized void gateway(DAGEventSchedulerUpdate event) {

    if (dagStartTime == -1L) {
      dagStartTime = System.currentTimeMillis();
    }

    if(null == event) {
      doClearOutPendingEvents();
    } else {
      doProcessEvent(event);
    }
  }

  private String getSubStageName(Vertex vertex, TezTaskID taskId) {

    TaskLocationHint hint = vertex.getTaskLocationHint(taskId);
    String nodeName = "null";

    if (hint != null && hint.getHosts() != null && hint.getHosts().size() > 0) {
      nodeName = (String) hint.getHosts().toArray()[0];
    }

    return vertex.getName() + "-" + nodeName;
  }

  private void doProcessEvent(DAGEventSchedulerUpdate event) {
    // Receiving an event asking with what priority this task has to be
    // scheduled. If it has to be scheduled, a "TaskAttemptEventSchedule" is
    // raised.

    TaskAttempt attempt = event.getAttempt();
    Vertex vertex = dag.getVertex(attempt.getVertexID());
    int vertexDistanceFromRoot = vertex.getDistanceFromRoot();

    // natural priority. Handles failures and retries.
    int priorityLowLimit = (vertexDistanceFromRoot + 1) * 3;
    int priorityHighLimit = priorityLowLimit - 2;

    // Create a response event for this "schedule" request
    TaskAttemptEventSchedule attemptEvent
        = new TaskAttemptEventSchedule(attempt.getID(), priorityLowLimit,
        priorityHighLimit);

    // TODO see if this is needed
    taskAttemptSeen(vertex.getName(), attempt.getID());

    // Push the response in the pipeline, it will be cleared out during periodic sweep
    pendingEvents.put(vertex.getName(), attemptEvent);
    String subStageId = getSubStageName(vertex, attempt.getTaskID());
    subStagePendingEvents.put(subStageId, attemptEvent);
    stage2vertex.put(subStageId, vertex.getName());
    if(!vertex2stage.containsKey(vertex.getName()))
      vertex2stage.put(vertex.getName(), new HashSet<String>());
    vertex2stage.get(vertex.getName()).add(subStageId);


    if (!scheduledVertices.contains(vertex.getName())) {
      // To determine if tasks for this vertex can be scheduled.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Attempting to schedule vertex: " + vertex.getLogIdentifier() +
            " due to schedule event");
      }

      trySchedulingVertex(vertex);
      // No need to process down stream vertices if no events have been sent
    }
  }


  private void doClearOutPendingEvents() {
    Map<TezVertexID, Vertex> dag_vertices = dag.getVertices();
    LOG.info("Ping received from self-clocking thread");

    // Update if vertices satisfy ordering constraint, based on new scheduled vertices added in previous clock tick
    for(TezVertexID vertex_id : dag_vertices.keySet()) {
      Vertex vertex = dag_vertices.get(vertex_id);

      if(scheduledVertices.contains(vertex.getName()))
        continue;

      // Note the below function cannot push vertex to scheduled.
      // It can ensure if ordering is satisfied
      trySchedulingVertex(vertex);
      // Since current vertex is not scheduled, no point is exploring
      // down stream vertices
    }

    Long elapsedTime = System.currentTimeMillis() - dagStartTime;

    for(String subStageId : subStagePendingEvents.keySet()) {

      String vertexName = stage2vertex.get(subStageId);

      if(!_ordering_constraint_satisfied.get(vertexName))
        continue;

      Long stageThreshold = scheduleTimes.containsKey(subStageId) ? scheduleTimes.get(subStageId) : -1L;

      if(elapsedTime >= stageThreshold) {

        int num_events = sendEventsForSubStage(subStageId);

        LOG.info("Releasing pending events on timer trigger. " +
            ", Vertex Name = " + vertexName +
            ", Sub-Stage Name = " + subStageId +
            ", Threshold = " + stageThreshold +
            ", Time = " + elapsedTime);

        // Update the set of scheduled vertices
        scheduledSubStages.add(subStageId);

        int current = vertexResponses.containsKey(vertexName) ? vertexResponses.get(vertexName) : 0;
        vertexResponses.put(vertexName, current + num_events);

      } else {
        LOG.info("Time constraints still not satisfied. Holding for later." +
            ", Vertex Name = " + vertexName +
            ", Sub Stage Name = " + subStageId +
            ", Threshold = " + stageThreshold +
            ", Time = " + elapsedTime);
      }
    }

    for(TezVertexID vertex_id : dag_vertices.keySet()) {
      Vertex vertex = dag_vertices.get(vertex_id);
      if(vertexResponses.get(vertex.getName()) >= vertex.getTotalTasks()) {
        scheduledVertices.add(vertex.getName());
      }
    }

  }


}
