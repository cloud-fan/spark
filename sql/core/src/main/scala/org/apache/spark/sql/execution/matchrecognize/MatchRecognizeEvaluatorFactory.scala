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

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.RowPattern
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.unsafe.types.UTF8String

/**
 * Factory for creating MatchRecognize partition evaluators.
 *
 * Following the WindowEvaluatorFactory pattern, this creates evaluators
 * that process sorted partitions to find pattern matches and emit matched rows.
 */
class MatchRecognizeEvaluatorFactory(
    val partitionExprs: Seq[Expression],
    val partitionOutputAttrs: Seq[Attribute],
    val pattern: RowPattern,
    val patternVariableDefinitions: Seq[Alias],
    val matchNumberAttr: Attribute,
    val classifierAttr: Attribute,
    val childOutput: Seq[Attribute],
    val numMatchesMetric: SQLMetric)
  extends PartitionEvaluatorFactory[InternalRow, InternalRow] {

  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] = {
    new MatchRecognizePartitionEvaluator()
  }

  /**
   * Evaluator that processes a single partition to find pattern matches.
   */
  class MatchRecognizePartitionEvaluator extends PartitionEvaluator[InternalRow, InternalRow] {

    override def eval(
        partitionIndex: Int,
        inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      val stream = inputs.head

      new MatchRecognizeIterator(
        stream,
        partitionExprs,
        partitionOutputAttrs,
        pattern,
        patternVariableDefinitions,
        matchNumberAttr,
        classifierAttr,
        childOutput,
        numMatchesMetric,
        partitionIndex
      )
    }
  }
}

/**
 * Iterator that processes a sorted partition and emits match results.
 *
 * This iterator coordinates the pattern matching process:
 * 1. Handles input iteration and partition boundary detection
 * 2. Delegates core matching logic to MatchingEngine
 * 3. Builds output rows for the matched branch
 *
 * Output rows contain partition columns, internal columns, and matched row columns.
 */
class MatchRecognizeIterator(
    input: Iterator[InternalRow],
    partitionExprs: Seq[Expression],
    partitionOutputAttrs: Seq[Attribute],
    pattern: RowPattern,
    patternVariableDefinitions: Seq[Alias],
    matchNumberAttr: Attribute,
    classifierAttr: Attribute,
    childOutput: Seq[Attribute],
    numMatchesMetric: SQLMetric,
    partitionIndex: Int) extends Iterator[InternalRow] {

  // Compile the state machine (predicates are initialized during compilation)
  private val stateMachine = PatternStateMachine.compile(
    pattern, patternVariableDefinitions, childOutput, partitionIndex)

  // Core matching engine handles branch management and tie-breaking
  private val matchingEngine = new MatchingEngine(stateMachine)

  // Projection for partitioning (to detect partition changes)
  private val grouping = UnsafeProjection.create(partitionExprs, childOutput)
  private val groupEqualityCheck: (UnsafeRow, UnsafeRow) => Boolean = {
    if (partitionExprs.forall(e => UnsafeRowUtils.isBinaryStable(e.dataType))) {
      (key1: UnsafeRow, key2: UnsafeRow) => key1.equals(key2)
    } else {
      val types = partitionExprs.map(_.dataType)
      val ordering = InterpretedOrdering.forSchema(types)
      (key1: UnsafeRow, key2: UnsafeRow) => ordering.compare(key1, key2) == 0
    }
  }

  private val outputProjection = UnsafeProjection.create(
    (partitionOutputAttrs.map(_.dataType) ++
      Seq(matchNumberAttr.dataType, classifierAttr.dataType) ++
      childOutput.map(_.dataType)).toArray)

  // State for iteration
  private var currentRow: UnsafeRow = _
  private var currentGroup: UnsafeRow = _
  private var previousGroup: UnsafeRow = _
  private var nextRowAvailable: Boolean = false
  private var matchIdCounter: Long = 1L
  private var pendingOutput: Iterator[InternalRow] = Iterator.empty

  // Initialize by fetching first row
  fetchNextRow()
  // Initialize the previous group to the first partition
  previousGroup = if (currentGroup != null) currentGroup.copy() else null

  private def fetchNextRow(): Unit = {
    nextRowAvailable = input.hasNext
    if (nextRowAvailable) {
      currentRow = input.next().asInstanceOf[UnsafeRow]
      currentGroup = grouping(currentRow)
    } else {
      currentRow = null
      currentGroup = null
    }
  }

  override def hasNext: Boolean = {
    if (pendingOutput.hasNext) return true

    // Process rows until we find a match or exhaust input
    while (nextRowAvailable) {
      val matchResults = processCurrentRow()
      if (matchResults.hasNext) {
        pendingOutput = matchResults
        return true
      }
    }

    // Input exhausted - try to complete any in-progress match
    matchingEngine.endOfInput() match {
      case Some(matchState) =>
        numMatchesMetric.add(1)
        // Use previousGroup since currentGroup is null after input exhaustion
        val resultRows = buildMatchResultsWithGroup(matchState, previousGroup)
        pendingOutput = withCleanup(resultRows, () => matchingEngine.finalizeMatch())
        if (pendingOutput.hasNext) return true
      case None =>
        // No match to complete
    }

    // Clean up when done
    matchingEngine.reset()
    false
  }

  override def next(): InternalRow = {
    if (!hasNext) {
      throw new NoSuchElementException("No more matches")
    }
    pendingOutput.next()
  }

  /**
   * Processes the current row through the pattern matching algorithm.
   *
   * @return Some(result) if a match was found, None otherwise
   */
  private def processCurrentRow(): Iterator[InternalRow] = {
    // Check for partition change - if the partition has changed, complete pending and reset
    if (previousGroup != null && !groupEqualityCheck(currentGroup, previousGroup)) {
      // Save the old group for building results from the previous partition
      val oldGroup = previousGroup

      // Update state for new partition BEFORE building results
      // (buildMatchResultsWithGroup uses previousGroup for match ID)
      matchIdCounter = 1L
      previousGroup = currentGroup.copy()

      // Try to complete any pending match from the previous partition
      matchingEngine.endOfInput() match {
        case Some(matchState) =>
          numMatchesMetric.add(1)
          val resultRows = buildMatchResultsWithGroup(matchState, oldGroup)
          // Cleanup: finalize match, then reset for new partition
          return withCleanup(resultRows, () => {
            matchingEngine.finalizeMatch()
            matchingEngine.reset()
          })
        case None =>
          // No pending match - just reset for new partition
          matchingEngine.reset()
      }
    }

    // Delegate to matching engine
    val matchResult = matchingEngine.processRow(currentRow)

    // Build result BEFORE fetching next row, since we need currentGroup
    matchResult match {
      case Some(matchState) =>
        numMatchesMetric.add(1)
        val resultRows = buildMatchResults(matchState)
        withCleanup(resultRows, () => {
          matchingEngine.finalizeMatch()
          fetchNextRow()
        })
      case None =>
        fetchNextRow()
        Iterator.empty
    }
  }

  /**
   * Wraps an iterator with cleanup logic that runs after the last element is consumed.
   */
  private def withCleanup(
      inner: Iterator[InternalRow],
      cleanup: () => Unit): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      private var cleaned = false
      override def hasNext: Boolean = {
        val has = inner.hasNext
        if (!has && !cleaned) {
          cleanup()
          cleaned = true
        }
        has
      }
      override def next(): InternalRow = {
        if (!hasNext) throw new NoSuchElementException("No more elements")
        inner.next()
      }
    }
  }

  // Reusable JoinedRow for result building
  private val joinedRow = new JoinedRow
  private val joinedRowWithMatch = new JoinedRow
  private val matchInfoRow = new GenericInternalRow(2)

  /**
   * Builds output rows for a completed match using current group.
   */
  private def buildMatchResults(matchState: MatchState): Iterator[InternalRow] = {
    buildMatchResultsWithGroup(matchState, currentGroup)
  }

  /**
   * Builds output rows for a completed match with explicit group.
   */
  private def buildMatchResultsWithGroup(
      matchState: MatchState,
      group: UnsafeRow): Iterator[InternalRow] = {
    val rowStore = matchingEngine.getRowStore
    val matchId = matchIdCounter
    matchIdCounter += 1

    val matchHistory = matchState.getMatchHistory
    if (matchHistory.isEmpty) {
      return Iterator.empty
    }

    matchInfoRow.update(0, matchId)
    var currentOffset = matchState.startOffset
    matchHistory.iterator.flatMap { mp =>
      val classifier = UTF8String.fromString(mp.variableName)
      matchInfoRow.update(1, classifier)
      (0 until mp.count).iterator.map { _ =>
        val matchedRow = rowStore.getRow(currentOffset)
        currentOffset += 1
        val joined = joinedRowWithMatch(joinedRow(group, matchInfoRow), matchedRow)
        outputProjection(joined)
      }
    }
  }
}
