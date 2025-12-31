/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.matchrecognize

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.UnsafeRow

/**
 * Core pattern matching engine using NFA simulation.
 *
 * This class implements Thompson's NFA simulation algorithm, maintaining
 * multiple active match states in parallel. This provides:
 * - Guaranteed O(n * m) time complexity (n = rows, m = pattern states)
 * - No exponential blowup on any pattern
 * - Correct handling of greedy/reluctant quantifiers
 * - Natural support for nested groups via graph structure
 *
 * Key concepts:
 * - activeStates: Ordered list of all currently active MatchState instances
 * - Each row is processed against ALL active states simultaneously
 * - When multiple matches complete, the first completed state in the list wins
 *
 * Active states ordering:
 * - States are ordered by priority (first = highest priority)
 * - Earlier-starting states come before later-starting states
 * - Within the same start offset, greedy/reluctant determines order:
 *   - Greedy: matching (non-completed) states before completed states
 *   - Reluctant: completed states before matching states
 * - A match is returned when the first state in the list is completed
 *
 * @param stateMachine The compiled pattern state machine
 */
class MatchingEngine(stateMachine: PatternStateMachine) {

  private val rowStore = new RowStore()

  // All currently active match states (NFA simulation).
  // States are ordered by preference: for greedy patterns, matching states come before
  // completed states; for reluctant patterns, completed states come first.
  // Completed states stay in the list but don't progress on new rows.
  private val activeStates = ArrayBuffer.empty[MatchState]

  /**
   * Resets the engine state for a new partition.
   */
  def reset(): Unit = {
    rowStore.clear()
    activeStates.clear()
  }

  /**
   * Processes a single row through the NFA simulation.
   *
   * When a match completes, the current row may or may not be part of the match:
   * - Row consumed: The row matched a pattern variable and is included in the match.
   *   Example: Pattern `A+ B`, row matches B, pattern completes with row consumed.
   * - Row not consumed: The row triggered completion but is not part of the match.
   *   Example: Pattern `A+`, row doesn't match A, but A+ min is satisfied, so
   *   pattern completes without consuming the row.
   *
   * To determine if the current row was consumed, compare the row offset with
   * `matchState.endOffset`. If `rowOffset < endOffset`, the row was consumed.
   * The row offset can be obtained from `rowStore.getNextOffset - 1` after this call.
   *
   * Note: When a match completes, any unconsumed row is automatically given a chance
   * to start a new match attempt in the same call. This implements SKIP PAST LAST ROW
   * semantics where we continue matching from the first row after the match.
   *
   * @param row The input row to process
   * @return Some(matchState) if a match was found, None otherwise
   */
  def processRow(row: UnsafeRow): Option[MatchState] = {
    // Store the row and get its offset
    val rowOffset = rowStore.addRow(row)

    // Process all active states with this row, adding new starting state lazily
    stepAllStates(row, rowOffset)

    // Check if the first active state is completed - if so, return it.
    // The order of states respects greedy/reluctant semantics:
    // - Greedy: matching states come before completed states
    // - Reluctant: completed states come before matching states
    // We only return a match if the FIRST state is completed, because:
    // - For greedy, matching states come first - we wait until they die
    // - For reluctant, completed states come first - we return immediately
    activeStates.headOption match {
      case Some(state) if state.completed => Some(state)
      case _ => None
    }
  }

  /**
   * Steps all active states with the given row.
   *
   * For each state:
   * - Completed states don't progress (they stay in place)
   * - Non-completed states get transitions from the state machine
   * - Dead ends (no transitions) are removed
   *
   * The order of resulting states respects greedy/reluctant semantics:
   * - The state machine returns transitions in preference order
   * - We add states in that order, preserving the preference
   *
   * A new starting state is added lazily at the end (lowest priority) only if
   * early stop didn't trigger. This avoids creating a state that would be
   * immediately discarded.
   *
   * TODO: This method assumes AFTER MATCH SKIP PAST LAST ROW mode.
   * Once we add any completed state, we stop adding more states since they'll be pruned anyway.
   * Update when supporting other modes like SKIP TO NEXT ROW.
   */
  private def stepAllStates(row: UnsafeRow, rowOffset: Long): Unit = {
    val nextStates = ArrayBuffer.empty[MatchState]
    var hasCompleted = false

    // Process existing states first, then lazily create and process new starting state
    val existingIter = activeStates.iterator
    var newStateCreated = false

    while (!hasCompleted) {
      val state = if (existingIter.hasNext) {
        existingIter.next()
      } else if (!newStateCreated) {
        // Lazily create new starting state (lowest priority)
        newStateCreated = true
        MatchState(rowOffset, stateMachine.startNode)
      } else {
        // No more states to process
        hasCompleted = true // Exit loop
        null // Won't be used
      }

      if (state != null && !hasCompleted) {
        if (state.completed) {
          // Completed states don't progress - they stay in place
          nextStates += state
          hasCompleted = true
        } else {
          // Get transitions from state machine (in preference order)
          val transitions = stateMachine.getAllTransitions(state.nfaState, row)
          val transIter = transitions.iterator

          while (transIter.hasNext && !hasCompleted) {
            transIter.next() match {
              case PatternComplete(matchedVar, nfaState) =>
                val finalState = matchedVar match {
                  case Some(v) => state.recordMatchWithNFAState(v, nfaState).markCompleted
                  case None => state.withNFAState(nfaState).markCompleted
                }
                nextStates += finalState
                hasCompleted = true

              case MatchedTransition(nfaState, matchedVar) =>
                nextStates += state.recordMatchWithNFAState(matchedVar, nfaState)
            }
          }
          // If no transitions, state is dead end and is not added to nextStates
        }
      }
    }

    activeStates.clear()
    activeStates ++= nextStates

    // Prune row store based on updated active states
    pruneRowStore()
  }

  /**
   * Prunes the row store based on current state.
   */
  private def pruneRowStore(): Unit = {
    val minOffset = if (activeStates.nonEmpty) {
      activeStates.map(_.startOffset).min
    } else {
      rowStore.getNextOffset // Can clear everything
    }
    rowStore.pruneBeforeOffset(minOffset)
  }

  /**
   * Gets the current row store for building match results.
   */
  def getRowStore: RowStore = rowStore

  /**
   * Called when input ends to attempt completing any in-progress matches.
   *
   * For patterns like A+ where we're waiting for more rows, this checks
   * if any active state can complete the pattern and returns the best match.
   *
   * Completed states (already marked) are returned directly. Non-completed
   * states are checked via `canComplete` to see if they can complete without
   * consuming more rows.
   *
   * Important: When a completed state is returned, rows processed after that
   * completion are NOT part of the returned match. Use `matchState.endOffset`
   * to determine the actual match boundary. For example:
   * - Pattern: ((A B)+ C)+
   * - Input: A B C A (then endOfInput)
   * - Greedy loops after first ABC, marks that as completed
   * - Second iteration gets A but is incomplete
   * - endOfInput returns the completed ABC state with endOffset=3
   * - The final A (at offset 3) was NOT consumed by the returned match
   *
   * @return Some(matchState) if a match can be completed, None otherwise
   */
  def endOfInput(): Option[MatchState] = {
    // Find the first completable state (highest priority due to ordering):
    // - Already completed states
    // - Non-completed states that can complete without more input
    val firstCompletable = activeStates.collectFirst {
      case state if state.completed => state
      case state if stateMachine.canComplete(state.nfaState) => state.markCompleted
    }

    firstCompletable match {
      case Some(completedState) =>
        // Update active states to just this one and prune row store
        activeStates.clear()
        activeStates += completedState
        pruneRowStore()
        Some(completedState)
      case None =>
        // No match possible - clear everything
        activeStates.clear()
        pruneRowStore()
        None
    }
  }

  /**
   * Called after the caller has consumed the returned match.
   * Removes the consumed state from the active list and prunes the row store.
   *
   * For SKIP PAST LAST ROW mode, other states are already pruned in stepAllStates,
   * so the list only contains the consumed state.
   */
  def finalizeMatch(): Unit = {
    // Remove the consumed state (should be the only one for SKIP PAST LAST ROW)
    activeStates.remove(0)
    pruneRowStore()
  }
}
