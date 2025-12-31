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

/**
 * Run-length encoded match entry for a pattern variable.
 *
 * @param variableName The pattern variable name (e.g., A, B)
 * @param count Number of consecutive rows matched by the variable
 */
case class MatchedPattern(variableName: String, count: Int)

/**
 * Immutable state representing one active match attempt in NFA simulation.
 *
 * In NFA simulation, we maintain multiple MatchState instances in parallel,
 * one for each possible path through the pattern. This requires MatchState
 * to be immutable so that forking a state doesn't affect the original.
 *
 * Match history is stored in run-length encoded form for efficiency.
 * The "pending" variable optimization avoids:
 * 1. Creating new MatchedPattern objects on every row
 * 2. Updating the immutable Vector on every row (which requires allocation)
 * Instead, we only append to the Vector when the matched variable changes.
 *
 * A completed state has matched the full pattern and is waiting to be returned.
 * Completed states stay in the active list but don't progress on new rows.
 * The order of states in the active list determines which match to return:
 * the first completed state wins.
 *
 * @param startOffset The global row offset where this match attempt started
 * @param nfaState Current NFA state (node + match/group counts)
 * @param matchHistory Completed run-length encoded variable matches
 * @param pendingVariable Current variable being matched (not yet in history)
 * @param pendingCount Count for the pending variable
 * @param completed Whether this state has completed the pattern
 */
case class MatchState(
    startOffset: Long,
    nfaState: NFAState,
    matchHistory: Vector[MatchedPattern],
    pendingVariable: String,
    pendingCount: Int,
    completed: Boolean = false) {

  /**
   * Returns the complete match history (including pending).
   */
  def getMatchHistory: Seq[MatchedPattern] = {
    if (pendingCount > 0) {
      matchHistory :+ MatchedPattern(pendingVariable, pendingCount)
    } else {
      matchHistory
    }
  }

  /**
   * Returns the total number of rows matched.
   */
  def totalMatchedRows: Int = {
    matchHistory.map(_.count).sum + pendingCount
  }

  /**
   * Returns the exclusive end offset (one past the last matched row).
   *
   * The matched rows span from `startOffset` (inclusive) to `endOffset` (exclusive).
   * Use this to determine:
   * - The skip offset for AFTER MATCH SKIP PAST LAST ROW
   * - Whether a row was consumed: if `rowOffset < endOffset`, the row is in the match
   */
  def endOffset: Long = startOffset + totalMatchedRows

  /**
   * Creates a new MatchState with the given NFA state.
   */
  def withNFAState(newNFAState: NFAState): MatchState = {
    copy(nfaState = newNFAState)
  }

  /**
   * Records a match and updates NFA state in a single copy.
   * More efficient than calling recordMatch().withNFAState() separately.
   */
  def recordMatchWithNFAState(variableName: String, newNFAState: NFAState): MatchState = {
    if (pendingCount > 0 && pendingVariable == variableName) {
      copy(nfaState = newNFAState, pendingCount = pendingCount + 1)
    } else {
      val newHistory = if (pendingCount > 0) {
        matchHistory :+ MatchedPattern(pendingVariable, pendingCount)
      } else {
        matchHistory
      }
      copy(
        nfaState = newNFAState,
        matchHistory = newHistory,
        pendingVariable = variableName,
        pendingCount = 1
      )
    }
  }

  /**
   * Gets the count for a specific node (MatchNode or AltNode) from the NFA position.
   */
  def getCount(nodeId: Int): Int = {
    nfaState.getCount(nodeId)
  }

  /**
   * Marks this state as completed, indicating it has matched the full pattern.
   */
  def markCompleted: MatchState = copy(completed = true)

  override def toString: String = {
    val historyString = getMatchHistory.map { mp =>
      s"${mp.variableName}(${mp.count})"
    }.mkString("[", ", ", "]")
    s"MatchState(start=$startOffset, history=$historyString)"
  }
}

object MatchState {
  /**
   * Creates a new MatchState starting at the given offset with the start NFA position.
   */
  def apply(startOffset: Long, startState: NFANode): MatchState = {
    MatchState(
      startOffset = startOffset,
      nfaState = NFAState(startState),
      matchHistory = Vector.empty,
      pendingVariable = null,
      pendingCount = 0
    )
  }
}
