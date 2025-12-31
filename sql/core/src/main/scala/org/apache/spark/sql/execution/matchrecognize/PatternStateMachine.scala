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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, BasePredicate}
import org.apache.spark.sql.catalyst.expressions.{BindReferences, Predicate}
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * A node in the pattern matching NFA graph.
 *
 * Node types:
 * - MatchNode: consumes a row if predicate matches
 * - AltNode: epsilon split (choice between two paths)
 * - AcceptNode: final accepting node (pattern complete)
 *
 * This graph-based design naturally handles nested groups like ((A B)+ C)+
 * without complex metadata on elements.
 */
sealed trait NFANode extends Serializable {
  /** Unique identifier for this node. AcceptNode uses -1 as a sentinel value. */
  def id: Int
}

/**
 * A node that matches a row against a predicate.
 *
 * Note: This is a regular class (not case class) because the NFA graph has cycles
 * (e.g., A+ loops back). Case class auto-generated hashCode/equals would cause
 * stack overflow by recursively traversing the cyclic graph.
 *
 * @param id Unique identifier for this node (used for match count tracking)
 * @param variableName The pattern variable name
 * @param predicate Compiled predicate to evaluate
 * @param minOccurrences Minimum matches required at this node
 * @param maxOccurrences Maximum matches allowed (None = unbounded)
 * @param greedy Whether to prefer more matches
 */
class MatchNode(
    val id: Int,
    val variableName: String,
    val predicate: BasePredicate,
    val minOccurrences: Int,
    val maxOccurrences: Option[Int],
    val greedy: Boolean) extends NFANode {

  var next: NFANode = _
  var stay: NFANode = _

  override def toString: String = s"MatchNode($id, $variableName)"
}

object MatchNode {
  def apply(
      id: Int,
      variableName: String,
      predicate: BasePredicate,
      minOccurrences: Int,
      maxOccurrences: Option[Int],
      greedy: Boolean,
      next: NFANode,
      stay: NFANode): MatchNode = {
    val node = new MatchNode(id, variableName, predicate, minOccurrences, maxOccurrences, greedy)
    node.next = next
    node.stay = stay
    node
  }
}

/**
 * An epsilon transition node that provides a choice between two paths.
 * Used at group END boundaries to choose between loop and exit.
 *
 * Note: This is a regular class (not case class) to avoid cyclic hashCode issues.
 *
 * @param id Unique identifier for this node (shared ID space with MatchNode)
 * @param greedy If true, prefer loop path; if false, prefer exit path
 */
class AltNode(
    val id: Int,
    val greedy: Boolean) extends NFANode {

  var loopPath: NFANode = _
  var exitPath: NFANode = _
  var loopNodeIds: Set[Int] = Set.empty

  override def toString: String = s"AltNode(id=$id, greedy=$greedy)"
}

object AltNode {
  def apply(
      id: Int,
      loopPath: NFANode,
      exitPath: NFANode,
      greedy: Boolean,
      loopNodeIds: Set[Int] = Set.empty): AltNode = {
    val node = new AltNode(id, greedy)
    node.loopPath = loopPath
    node.exitPath = exitPath
    node.loopNodeIds = loopNodeIds
    node
  }
}

/**
 * The final accepting node. Pattern is complete.
 */
case object AcceptNode extends NFANode {
  override val id: Int = -1
}

/**
 * Tracks the runtime state during NFA traversal.
 *
 * @param node Current node in the NFA graph
 * @param counts Occurrence counts per node id (MatchNode for quantifiers, AltNode for groups)
 */
case class NFAState(
    node: NFANode,
    counts: Map[Int, Int] = Map.empty) {

  def getCount(nodeId: Int): Int = counts.getOrElse(nodeId, 0)

  def incrementCount(nodeId: Int): NFAState = {
    copy(counts = counts.updated(nodeId, getCount(nodeId) + 1))
  }
}

/**
 * The result of trying a transition.
 */
sealed trait TransitionResult

/**
 * Successfully matched the current row.
 * @param state The new NFAState after the transition
 * @param matchedVariable The variable name that matched
 */
case class MatchedTransition(state: NFAState, matchedVariable: String)
    extends TransitionResult

/**
 * Pattern is complete, emit result.
 * @param matchedVariable If Some(name), record the current row as matching this variable.
 *                        If None, don't record the current row (it didn't match).
 * @param state The final NFAState (for group counts)
 */
case class PatternComplete(matchedVariable: Option[String], state: NFAState)
    extends TransitionResult


/**
 * A compiled NFA state machine for pattern matching.
 *
 * Uses Thompson-style NFA construction where:
 * - Quantifiers become MatchNodes with self-loops
 * - Groups become AltNodes for loop/exit choice
 * - Nested groups are naturally represented as nested AltNodes
 *
 * @param startNode The entry point of the NFA
 */
class PatternStateMachine(val startNode: NFANode) extends Serializable {

  /**
   * Gets all valid transitions from the given state for the input row.
   *
   * This handles NFA simulation by exploring all reachable states:
   * - MatchNode: try to match, return transitions based on greedy/reluctant
   * - AltNode: epsilon closure - explore both branches
   * - AcceptNode: pattern complete
   *
   * @param state Current NFA state
   * @param row The input row to evaluate
   * @return Sequence of all valid TransitionResults
   */
  def getAllTransitions(
      state: NFAState,
      row: InternalRow): Seq[TransitionResult] = {
    // The state is always at a MatchNode or AcceptNode because:
    // - startNode is always a MatchNode (groups compile to start at first element)
    // - All transitions return states at MatchNodes (via epsilon closure in
    //   computeAdvanceTransitions) or AcceptNode
    tryTransitionFromState(state, row)
  }

  /**
   * Try transitions from a state at a MatchNode or AcceptNode.
   */
  private def tryTransitionFromState(
      state: NFAState,
      row: InternalRow): Seq[TransitionResult] = {

    state.node match {
      case AcceptNode =>
        // Already at accept state - pattern complete without consuming this row
        Seq(PatternComplete(None, state))

      case m: MatchNode =>
        tryMatchNodeTransition(state, m, row)

      case alt: AltNode =>
        throw SparkException.internalError(
          s"AltNode(id=${alt.id}) should have been handled by epsilon closure")
    }
  }

  /**
   * Try transitions from a state at MatchNode.
   */
  private def tryMatchNodeTransition(
      state: NFAState,
      node: MatchNode,
      row: InternalRow): Seq[TransitionResult] = {

    val currentCount = state.getCount(node.id)

    // Max should never be reached before consuming a row - if max was reached after
    // the previous match, the state should have already advanced to the next node.
    assert(node.maxOccurrences.forall(currentCount < _),
      s"MatchNode(id=${node.id}) reached with count=$currentCount >= max=${node.maxOccurrences}")

    val rowMatches = node.predicate.eval(row)

    if (rowMatches) {
      handleMatchingRow(state, node, currentCount)
    } else if (currentCount >= node.minOccurrences) {
      // Row doesn't match but min satisfied - try advancing
      advanceToNext(state, node, row)
    } else {
      // Row doesn't match and min not satisfied - dead end.
      Seq.empty
    }
  }

  /**
   * Handle a row that matches the current MatchNode.
   */
  private def handleMatchingRow(
      state: NFAState,
      node: MatchNode,
      currentCount: Int): Seq[TransitionResult] = {

    val newCount = currentCount + 1
    val newMaxReached = node.maxOccurrences.exists(_ <= newCount)
    val newMinSatisfied = newCount >= node.minOccurrences
    val stateWithNodeCount = state.incrementCount(node.id)

    // If next leads to AltNode(s), increment counts for all of them.
    // This makes group counts accurate at all times - when we match the last
    // element of a group, we've completed that group iteration (and any outer
    // groups whose last element is this inner group).
    val exitAltNodes = collectExitAltNodes(node.next)
    val newState = exitAltNodes.foldLeft(stateWithNodeCount) { (s, alt) =>
      s.incrementCount(alt.id)
    }

    if (newMaxReached) {
      // Must advance after this match
      computeAdvanceTransitions(newState, node)
    } else {
      // Can stay or advance
      val stayTransition = MatchedTransition(
        newState.copy(node = if (node.stay != null) node.stay else node),
        node.variableName)

      if (!newMinSatisfied) {
        // Min not satisfied, must stay
        Seq(stayTransition)
      } else {
        // Both stay and advance are valid
        val advanceTransitions = computeAdvanceTransitions(newState, node)

        if (node.greedy) {
          // Greedy: stay is preferred, but for NFA we explore both
          // Return all transitions - engine handles greedy/reluctant logic
          Seq(stayTransition) ++ advanceTransitions
        } else {
          // Reluctant: advance is preferred, stay is alternative
          advanceTransitions :+ stayTransition
        }
      }
    }
  }

  /**
   * Collects all AltNodes reachable via exit paths from the given node.
   * This is used to increment group counts for all groups that are completed
   * when matching the last element before an AltNode chain.
   *
   * For example, for pattern (A (B C)+)+:
   * - After matching C, C.next = innerAlt, and innerAlt.exitPath = outerAlt
   * - This method returns Seq(innerAlt, outerAlt) starting from C.next
   */
  private def collectExitAltNodes(node: NFANode): Seq[AltNode] = {
    val result = scala.collection.mutable.ArrayBuffer[AltNode]()
    var current = node
    while (current.isInstanceOf[AltNode]) {
      val alt = current.asInstanceOf[AltNode]
      result += alt
      current = alt.exitPath
    }
    result.toSeq
  }

  /**
   * Compute transitions for advancing to the next state after a match.
   */
  private def computeAdvanceTransitions(
      state: NFAState,
      node: MatchNode): Seq[TransitionResult] = {

    if (node.next == AcceptNode) {
      Seq(PatternComplete(Some(node.variableName), state.copy(node = AcceptNode)))
    } else {
      // Compute epsilon closure from next state
      val nextState = state.copy(node = node.next)
      val reachable = epsilonClosure(nextState)

      val transitions = reachable.flatMap { reachableState =>
        reachableState.node match {
          case AcceptNode =>
            Seq(PatternComplete(Some(node.variableName), reachableState))
          case _: MatchNode =>
            // For NFA, we return the transition to this state
            Seq(MatchedTransition(reachableState, node.variableName))
          case _ =>
            Seq.empty
        }
      }

      // Return all transitions - engine handles greedy/reluctant logic
      transitions
    }
  }

  /**
   * Computes epsilon closure - all states reachable via epsilon transitions.
   * Epsilon transitions are AltNodes (choices) that don't consume input.
   *
   * When taking the loop path through an AltNode, match counts for nodes
   * in that loop are reset (to allow re-matching in the next iteration).
   */
  private def epsilonClosure(state: NFAState): Seq[NFAState] = {
    val result = ArrayBuffer.empty[NFAState]
    val visited = scala.collection.mutable.Set.empty[Int]

    def explore(current: NFAState): Unit = {
      if (visited.contains(current.node.id)) return
      visited += current.node.id

      current.node match {
        case alt: AltNode =>
          // For loop path, reset counts for inner nodes (both MatchNodes and AltNodes)
          val loopState = if (alt.loopNodeIds.nonEmpty) {
            current.copy(counts = current.counts -- alt.loopNodeIds)
          } else {
            current
          }

          // Explore paths in preference order based on greedy/reluctant
          // Greedy: loopPath first (prefer to continue matching)
          // Reluctant: exitPath first (prefer to complete/exit)
          val loopNode = alt.loopPath
          val exitNode = alt.exitPath

          if (alt.greedy) {
            // Greedy: explore loop first, then exit
            if (loopNode != null) explore(loopState.copy(node = loopNode))
            if (exitNode != null) explore(current.copy(node = exitNode))
          } else {
            // Reluctant: explore exit first, then loop
            if (exitNode != null) explore(current.copy(node = exitNode))
            if (loopNode != null) explore(loopState.copy(node = loopNode))
          }

        case _: MatchNode | AcceptNode =>
          result += current
      }
    }

    explore(state)
    result.toSeq
  }

  /**
   * Try advancing to the next state when current row doesn't match.
   */
  private def advanceToNext(
      state: NFAState,
      node: MatchNode,
      row: InternalRow): Seq[TransitionResult] = {

    if (node.next == AcceptNode) {
      Seq(PatternComplete(None, state))
    } else {
      // Try to match from the next state
      val nextState = state.copy(node = node.next)
      getAllTransitions(nextState, row)
    }
  }

  /**
   * Checks if the pattern can complete from the given state without consuming more rows.
   */
  def canComplete(state: NFAState): Boolean = {
    canCompleteFromState(state, Set.empty)
  }

  private def canCompleteFromState(state: NFAState, visited: Set[Int]): Boolean = {
    if (visited.contains(state.node.id)) return false
    val newVisited = visited + state.node.id

    state.node match {
      case AcceptNode => true

      case alt: AltNode =>
        val groupCount = state.getCount(alt.id)
        if (groupCount >= 1) {
          // Group min satisfied, can take either path
          canCompleteFromState(state.copy(node = alt.exitPath), newVisited) ||
            canCompleteFromState(state.copy(node = alt.loopPath), newVisited)
        } else {
          // Group min not satisfied, must take loop path
          canCompleteFromState(state.copy(node = alt.loopPath), newVisited)
        }

      case m: MatchNode =>
        val currentCount = state.getCount(m.id)
        if (currentCount >= m.minOccurrences) {
          canCompleteFromState(state.copy(node = m.next), newVisited)
        } else {
          // Min not satisfied. We cannot complete from here.
          false
        }
    }
  }
}

object PatternStateMachine {

  /**
   * Creates a compiled NFA state machine from pattern expression.
   *
   * Uses Thompson's construction to build an NFA graph where:
   * - Single variables become MatchNodes
   * - Quantified variables have self-loops
   * - Groups are wrapped with AltNodes for loop/exit choice
   *
   * @param pattern The pattern expression AST
   * @param patternVariableDefinitions Definitions for pattern variables
   * @param childOutput The schema of the input data
   * @return A compiled PatternStateMachine
   */
  def compile(
      pattern: RowPattern,
      patternVariableDefinitions: Seq[Alias],
      childOutput: Seq[Attribute],
      partitionIndex: Int): PatternStateMachine = {

    // Convert definitions to a map
    val definitionsMap = patternVariableDefinitions.map(a => a.name -> a.child).toMap

    // Create and initialize predicates for each variable
    val predicateMap = definitionsMap.map { case (name, condition) =>
      val boundCondition = BindReferences.bindReference(condition, childOutput)
      val predicate = Predicate.create(boundCondition, childOutput)
      predicate.initialize(partitionIndex)
      name -> predicate
    }

    val context = new CompileContext(predicateMap)
    val startNode = compilePattern(pattern, AcceptNode, context)

    new PatternStateMachine(startNode)
  }

  /**
   * Compiles a pattern into NFA nodes, with 'next' as the continuation.
   */
  private def compilePattern(
      pattern: RowPattern,
      next: NFANode,
      context: CompileContext): NFANode = {

    pattern match {
      case PatternVariable(name) =>
        val predicate = context.getPredicate(name)
        val id = context.nextNodeId()
        // Single variable: exactly one occurrence
        MatchNode(
          id = id,
          variableName = name,
          predicate = predicate,
          minOccurrences = 1,
          maxOccurrences = Some(1),
          greedy = true,
          next = next,
          stay = null)

      case PatternSequence(patterns) =>
        // Build sequence from right to left, connecting each pattern to the next
        patterns.foldRight(next) { (p, continuation) =>
          compilePattern(p, continuation, context)
        }

      case QuantifiedPattern(inner, quantifier, greedy) =>
        inner match {
          case PatternVariable(name) =>
            // Quantified single variable
            val predicate = context.getPredicate(name)
            val id = context.nextNodeId()
            val node = MatchNode(
              id = id,
              variableName = name,
              predicate = predicate,
              minOccurrences = quantifier.minOccurrences,
              maxOccurrences = quantifier.maxOccurrences,
              greedy = greedy,
              next = next,
              stay = null)
            // Self-loop for quantifier
            if (quantifier.maxOccurrences.isEmpty || quantifier.maxOccurrences.get > 1) {
              node.stay = node
            }
            node

          case _ =>
            // Quantified group: (A B)+
            compileQuantifiedGroup(inner, quantifier, greedy, next, context)
        }

      case _: PatternAlternation =>
        throw SparkException.internalError(
          "Pattern alternation should have been rejected by CheckAnalysis")
    }
  }

  /**
   * Compiles a quantified group pattern like (A B)+.
   *
   * Thompson construction for (inner)+ with greedy:
   * - inner connects to AltNode
   * - AltNode.first loops back to inner (greedy prefers loop)
   * - AltNode.second exits to next
   *
   * The AltNode provides the loop/exit choice after each iteration.
   */
  private def compileQuantifiedGroup(
      inner: RowPattern,
      quantifier: PatternQuantifier,
      greedy: Boolean,
      next: NFANode,
      context: CompileContext): NFANode = {

    // Allocate ID for AltNode (shares ID space with MatchNodes)
    val altNodeId = context.nextNodeId()

    // Create AltNode placeholder (we'll set edges after compiling inner)
    val altNode = new AltNode(altNodeId, greedy)

    // Compile inner pattern with AltNode as continuation
    val innerStart = compilePattern(inner, altNode, context)

    // Collect all node IDs created for the inner pattern (for resetting on loop)
    val loopNodeIds = (altNodeId + 1).until(context.currentNodeId).toSet

    // Set up AltNode edges
    altNode.loopPath = innerStart
    altNode.exitPath = next
    altNode.loopNodeIds = loopNodeIds

    // TODO: Implement min/max constraints for group quantifiers (min/max repetitions).
    // Currently only + (min=1, unbounded max) works correctly by default.
    // Need to add minOccurrences/maxOccurrences to AltNode and check counts at runtime.
    innerStart
  }

  /**
   * Compilation context for tracking state during pattern compilation.
   */
  private class CompileContext(predicateMap: Map[String, BasePredicate]) {
    private var nodeIdCounter = 0

    def nextNodeId(): Int = {
      val id = nodeIdCounter
      nodeIdCounter += 1
      id
    }

    def currentNodeId: Int = nodeIdCounter

    def getPredicate(name: String): BasePredicate = {
      predicateMap.getOrElse(name,
        throw SparkException.internalError(s"No predicate for variable: $name"))
    }
  }
}
