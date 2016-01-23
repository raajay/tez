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
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.DAGScheduler;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdateTAAssigned;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventSchedule;
import org.apache.tez.dag.records.TezTaskAttemptID;
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

  // Tracks pending events, in case they're not sent immediately.
  private final ListMultimap<String, TaskAttemptEventSchedule> pendingEvents =
      LinkedListMultimap.create();

  // Tracks vertices for which no additional scheduling checks are required. Once in this list, the
  // vertex is considered to be fully scheduled.
  private final Set<String> scheduledVertices = new HashSet<String>();

  // Track the completed vertices, for which the even has been received
  private final Set<String> completedVertices = new HashSet<String>();

  // Tracks the tasks scheduled for vertices. That is the tasks for which the
  // schedule event has been already sent
  private final Map<String, BitSet> vertexScheduledTasks = new HashMap<String, BitSet>();

  // Tracks vertex schedule time relative to first vertex dagStartTime. Is
  // populated upon construction of the object
  private Map<String, Long> vertexScheduleTimes;

  private Long dagStartTime = -1L;

  private ScheduledThreadPoolExecutor _executor;
  private PendingDagEventProcessor _event_processor;
  private Map<String, Boolean> _ordering_constraint_satisfied;


  private void init() {
    this.vertexScheduleTimes = new HashMap<>();
    this._ordering_constraint_satisfied = new HashMap<>();
    Map<TezVertexID, Vertex> dag_vertices = dag.getVertices();
    for(TezVertexID vertex_id : dag_vertices.keySet()) {
      Vertex vertex = dag_vertices.get(vertex_id);
      String name = vertex.getName();
      vertexScheduleTimes.put(name, -1L);
      _ordering_constraint_satisfied.put(name, false);
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
        vertexScheduleTimes.put(vt[0], Long.parseLong(vt[1]));
      }
      reader.close();
    } catch (IOException e) {
      LOG.error("CQ: Reading the schedule file failed. File = " + schedule_file +
          "\n" + e.getMessage());
    }
  }

  /**
   * @param dag
   * @param dispatcher
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


  @Override
  public void vertexCompleted(Vertex vertex) {
    this.completedVertices.add(vertex.getName());
  }

  // TODO Does ordering matter - it currently depends on the order returned by vertex.getOutput*
  @Override
  public void scheduleTask(DAGEventSchedulerUpdate event) {
    doStuff(event);
  }

  /**
   * Update the set of tasks for which "request" has arrived for each vertex.
   * A vertex with no "request" will never come here, unless forced with a null
   * value, just to make its BitSet as zero.
   *
   * @param vertexName
   * @param taskAttemptID
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


  /**
   * Send and remove all the pending events, for a vertex.
   * @param vertexName
   */
  private void sendEventsForVertex(String vertexName) {
    for (TaskAttemptEventSchedule event : pendingEvents.removeAll(vertexName)) {
      sendEvent(event);
    }
  }


  /**
   * Checks whether this vertex has been marked as ready to go in the past
   * @param vertex
   * @return Indicator if the vertex is already scheduled.
   */
  private boolean vertexAlreadyScheduled(Vertex vertex) {
    return scheduledVertices.contains(vertex.getName());
  }


  /**
   * @param vertex
   * @return
   */
  private boolean scheduledTasksForwarded(Vertex vertex) {

    boolean canSchedule = false;

    BitSet scheduledTasks = vertexScheduledTasks.get(vertex.getName());
    if (scheduledTasks != null) { // At least one request is seen, none if none are seen
      // or is zero task vertex

      if (scheduledTasks.cardinality() >= vertex.getTotalTasks()) {
        // All task "request" are seen
        canSchedule = true;
      }

    }
    return canSchedule;
  }


  /**
   * @param vertex
   */
  private void processDownstreamVertices(Vertex vertex) {

    List<Vertex> newlyScheduledVertices = Lists.newLinkedList();
    Map<Vertex, Edge> outputVertexEdgeMap = vertex.getOutputVertices();

    for (Vertex destVertex : outputVertexEdgeMap.keySet()) {

      if (vertexAlreadyScheduled(destVertex)) { // Nothing to do if already scheduled.

      }
      else {

        if (LOG.isDebugEnabled()) {
          LOG.debug("Attempting to schedule vertex: " + destVertex.getLogIdentifier() +
              " due to upstream event from " + vertex.getLogIdentifier());
        }

        boolean scheduled = trySchedulingVertex(destVertex);

        if (scheduled) {
          LOG.info("Scheduled vertex: " + destVertex.getLogIdentifier() +
              " due to upstream event from " + vertex.getLogIdentifier());
          sendEventsForVertex(destVertex.getName());
          newlyScheduledVertices.add(destVertex);
        }

      }
    }

    // Try scheduling all downstream vertices which were scheduled in this run.
    // Recurse
    for (Vertex downStreamVertex : newlyScheduledVertices) {
      processDownstreamVertices(downStreamVertex);
    }
  }


  /**
   * Process the specified vertex, and add it to the cache of scheduled vertices if it can be scheduled
   * @param vertex
   * @return True/False indicating if vertex can be scheduled (i.e. all
   * conditions are satisfied)
   */
  private boolean trySchedulingVertex(Vertex vertex) {

    boolean canSchedule = true;

    if (vertexScheduledTasks.get(vertex.getName()) == null) {
      // 1.  No scheduled requests seen yet. Do not mark this as ready.
      // 0 task vertices handled elsewhere. DO NOT SCHEDULE
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "No schedule requests for vertex: " + vertex.getLogIdentifier() + ", Not scheduling");
      }

      canSchedule = false;

    } else {

      Map<Vertex, Edge> inputVertexEdgeMap = vertex.getInputVertices();

      if (inputVertexEdgeMap == null || inputVertexEdgeMap.isEmpty()) {
        // 2. Is a map vertex. Can be scheduled based on ordering constraint
        // alone.

        if (LOG.isDebugEnabled()) {
          LOG.debug("No sources for vertex: " + vertex.getLogIdentifier() + ", Scheduling now");
        }

      } else {

        // Check if all sources are scheduled.
        for (Vertex srcVertex : inputVertexEdgeMap.keySet()) {

          if (scheduledTasksForwarded(srcVertex)) {

            // 3. This source has already been okayed to be scheduled.

            if (LOG.isDebugEnabled()) {
              LOG.debug("Trying to schedule: " + vertex.getLogIdentifier() +
                  ", All tasks forwarded for srcVertex: " + srcVertex.getLogIdentifier() +
                  ", count: " + srcVertex.getTotalTasks());
            }


          } else {

            // Special case for vertices with 0 tasks. 0 check is sufficient since parallelism cannot increase.
            if (srcVertex.getTotalTasks() == 0) {
              // 4. If parent has zero vertices, it will not satisfy (3)

              LOG.info("Vertex: " + srcVertex.getLogIdentifier() + " has 0 tasks. Marking as scheduled");

              // this is adding only the parent
              scheduledVertices.add(srcVertex.getName());

              taskAttemptSeen(srcVertex.getName(), null);

            } else {

              if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "Not all sources schedule requests complete while trying to schedule: " +
                        vertex.getLogIdentifier() + ", For source vertex: " +
                        srcVertex.getLogIdentifier() + ", Forwarded requests: " +
                        (vertexScheduledTasks.get(srcVertex.getName()) == null ? "null(0)" :
                            vertexScheduledTasks.get(srcVertex.getName()).cardinality()) +
                        " out of " + srcVertex.getTotalTasks());
              }

              canSchedule = false;
              break;
            }
          }
        } // end for -- over source vertices

      } // end if - not a map

    } // end if - some "request" have been received

    // Update if ordering constraint has been satisfied
    _ordering_constraint_satisfied.put(vertex.getName(), canSchedule);

    // Add an extra check to see if enough time has elapsed
    if (canSchedule) {
      // If time has not been set, set it; happens during the first vertex

      if (dagStartTime == -1L) {
        Long tmp = System.currentTimeMillis();
        dagStartTime = tmp;
      }

      Long currentTime = System.currentTimeMillis();
      Long elapsedTime = currentTime - dagStartTime;

      Long thresholdTime = vertexScheduleTimes.get(vertex.getName());

      LOG.info("Delayed Launch Log: Vertex " + vertex.getName() + ", DagStartTime = "
          + dagStartTime + ", CurrentTime = " + currentTime);
      LOG.info("Delayed Launch Log: Vertex " + vertex.getName() + ", ElapsedTime = "
          + elapsedTime + ", Threshold = " + thresholdTime);

      // If threshold is not defined or if defined sufficient time has passed
      // after start, then add the vertex to the scheduled vertices set.
      if (thresholdTime == null || thresholdTime == -1L || elapsedTime > thresholdTime) {
        scheduledVertices.add(vertex.getName());
        LOG.info("Time constraint also satisfied. Scheduling vertex : " + vertex.getName());
      }
      else {
        // Else, do not schedule vertex and notify that we cannot schedule the
        // vertex
        canSchedule = false;
        LOG.info("Time constraints not satisfied. Holding vertex to be scheduled later."
            + " Vertex = " + vertex.getName());
      }
    }
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


  public void clearPendingEvents() {
    doStuff(null);
  }

  private synchronized void doStuff(DAGEventSchedulerUpdate event) {
    if(null == event) {
      doClearOutPendingEvents();
    } else {
      doProcessEvent(event);
    }
  }

  private void doProcessEvent(DAGEventSchedulerUpdate event) {
    // Receiving an event asking with what priority this task has to be
    // scheduled. If it has to be schedules, a "TaskAttemptEventSchedule" is
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

    taskAttemptSeen(vertex.getName(), attempt.getID());

    if (vertexAlreadyScheduled(vertex)) {
      // Vertex previously marked ready for scheduling. Means the response can
      // be sent immediately.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Scheduling " + attempt.getID() + " between priorityLow: " + priorityLowLimit
            + " and priorityHigh: " + priorityHighLimit);
      }

      // Sending immediately.
      sendEvent(attemptEvent);

      // A new task coming in here could send us over the enough tasks scheduled limit.
      // We process downstream vertices, to see if any of the "requests" are
      // pending, and their responses can be sent
      processDownstreamVertices(vertex);

    } else {
      // To determine if response if tasks for this vertex can be scheduled.

      if (LOG.isDebugEnabled()) {
        LOG.debug("Attempting to schedule vertex: " + vertex.getLogIdentifier() +
            " due to schedule event");
      }

      boolean scheduled = trySchedulingVertex(vertex);

      if (scheduled) {

        LOG.info("Scheduled vertex: " + vertex.getLogIdentifier());

        // If ready to be scheduled, send out pending events and the current event.
        // Send events out for this vertex first. Then try scheduling downstream vertices.

        // Send out all pending events for this vertex
        sendEventsForVertex(vertex.getName());
        // Send out the current event
        sendEvent(attemptEvent);

        if (LOG.isDebugEnabled()) {
          LOG.debug("Processing downstream vertices for vertex: " + vertex.getLogIdentifier());
        }

        // Now that we have decided to send the "response" for this vertex, and
        // the down stream vertices can now be in scheduled state, we also send
        // events from down stream vertices.
        processDownstreamVertices(vertex);

      } else {
        // Queue the "request" for later processing.
        pendingEvents.put(vertex.getName(), attemptEvent);
      }
    }
  }


  private void doClearOutPendingEvents() {
    Map<TezVertexID, Vertex> dag_vertices = dag.getVertices();
    LOG.info("Ping received from self-clocking thread");

    Long elapsedTime = System.currentTimeMillis() - dagStartTime;

    for(TezVertexID vertex_id : dag_vertices.keySet()) {
      Vertex vertex = dag_vertices.get(vertex_id);

      if(!_ordering_constraint_satisfied.get(vertex.getName()))
        continue;

      if(scheduledVertices.contains(vertex.getName()))
        continue;

      // We consider vertices for which
      // 1. Vertex has satisfied the ordering requirements
      // 2. Vertex is not already scheduled

      if(elapsedTime >= vertexScheduleTimes.get(vertex.getName())) {

        sendEventsForVertex(vertex.getName());

        LOG.info("Releasing pending events on timer trigger. " +
            ", Name = " + vertex.getName() +
            ", Threshold = " + vertexScheduleTimes.get(vertex.getName()) +
            ", Time = " + elapsedTime);
      } else {

        LOG.info("Time constraints still not satisfied. Holding for later." +
            ", Name = " + vertex.getName() +
            ", Threshold = " + vertexScheduleTimes.get(vertex.getName()) +
            ", Time = " + elapsedTime);
      }
    }
  }

}
