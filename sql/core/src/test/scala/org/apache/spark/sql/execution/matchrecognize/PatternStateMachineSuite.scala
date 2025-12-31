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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{
  AtLeast,
  OneOrMore,
  PatternSequence,
  PatternVariable,
  QuantifiedPattern,
  RowPattern,
  ZeroOrMore
}
import org.apache.spark.sql.types.IntegerType

/**
 * Tests for PatternStateMachine compilation and transition logic.
 */
class PatternStateMachineSuite extends SparkFunSuite {

  // Helper extension methods for navigating NFA nodes in tests
  implicit class NFANodeOps(node: NFANode) {
    def asMatch: MatchNode = node.asInstanceOf[MatchNode]
    def asAlt: AltNode = node.asInstanceOf[AltNode]
  }

  // Helper to create a row with a single int value
  private def createRow(value: Int): InternalRow = {
    InternalRow(value)
  }

  // Helper to compile a pattern without predicates (for structure tests)
  private def compilePattern(pattern: RowPattern): PatternStateMachine = {
    val varNames = pattern.variableNames.toSeq
    val definitions = varNames.map(name => Alias(Literal(true), name)())
    PatternStateMachine.compile(pattern, definitions, Seq.empty, partitionIndex = 0)
  }

  // Compile pattern with predicates that match specific int values
  private def compilePatternWithPredicates(
      pattern: RowPattern,
      varToValue: Map[String, Int]): PatternStateMachine = {
    val varNames = pattern.variableNames.toSeq
    val valueAttr = AttributeReference("value", IntegerType)()
    val definitions = varNames.map { name =>
      val matchValue = varToValue.getOrElse(name, 0)
      Alias(EqualTo(valueAttr, Literal(matchValue)), name)()
    }
    PatternStateMachine.compile(pattern, definitions, Seq(valueAttr), partitionIndex = 0)
  }

  // ==================== Pattern Compilation Tests ====================

  test("compile - single variable A") {
    val pattern = PatternVariable("A")
    val sm = compilePattern(pattern)

    val a = sm.startNode.asMatch
    assert(a.variableName == "A")
    assert(a.minOccurrences == 1)
    assert(a.maxOccurrences == Some(1))
    assert(a.next == AcceptNode)
    assert(a.stay == null) // No self-loop for single occurrence
  }

  test("compile - sequence A B") {
    val pattern = PatternSequence(Seq(PatternVariable("A"), PatternVariable("B")))
    val sm = compilePattern(pattern)

    // A -> B -> Accept
    val a = sm.startNode.asMatch
    assert(a.variableName == "A")
    val b = a.next.asMatch
    assert(b.variableName == "B")
    assert(b.next == AcceptNode)
  }

  test("compile - quantified variable A+") {
    val pattern = QuantifiedPattern(PatternVariable("A"), OneOrMore, greedy = true)
    val sm = compilePattern(pattern)

    val a = sm.startNode.asMatch
    assert(a.variableName == "A")
    assert(a.minOccurrences == 1)
    assert(a.maxOccurrences.isEmpty) // unbounded
    assert(a.greedy)
    assert(a.stay == a) // Self-loop for quantifier
    assert(a.next == AcceptNode)
  }

  test("compile - quantified variable A*") {
    val pattern = QuantifiedPattern(PatternVariable("A"), ZeroOrMore, greedy = true)
    val sm = compilePattern(pattern)

    val a = sm.startNode.asMatch
    assert(a.variableName == "A")
    assert(a.minOccurrences == 0)
    assert(a.maxOccurrences.isEmpty)
    assert(a.greedy)
    assert(a.stay == a)
  }

  test("compile - reluctant quantifier A+?") {
    val pattern = QuantifiedPattern(PatternVariable("A"), OneOrMore, greedy = false)
    val sm = compilePattern(pattern)

    val a = sm.startNode.asMatch
    assert(a.variableName == "A")
    assert(a.minOccurrences == 1)
    assert(a.maxOccurrences.isEmpty)
    assert(!a.greedy)
  }

  test("compile - sequence A+ B") {
    val pattern = PatternSequence(Seq(
      QuantifiedPattern(PatternVariable("A"), OneOrMore, greedy = true),
      PatternVariable("B")
    ))
    val sm = compilePattern(pattern)

    // Structure: A(stay=self) -> B -> Accept
    val a = sm.startNode.asMatch
    assert(a.variableName == "A")
    assert(a.minOccurrences == 1)
    assert(a.maxOccurrences.isEmpty)
    assert(a.greedy)
    assert(a.stay == a) // self-loop for A+

    val b = a.next.asMatch
    assert(b.variableName == "B")
    assert(b.minOccurrences == 1)
    assert(b.maxOccurrences == Some(1))
    assert(b.stay == null) // no self-loop for single B
    assert(b.next == AcceptNode)
  }

  test("compile - grouped pattern (A B)+") {
    val inner = PatternSequence(Seq(PatternVariable("A"), PatternVariable("B")))
    val pattern = QuantifiedPattern(inner, OneOrMore, greedy = true)
    val sm = compilePattern(pattern)

    // Structure: A -> B -> AltNode(loop to A, exit to Accept)
    val a = sm.startNode.asMatch
    assert(a.variableName == "A")

    val b = a.next.asMatch
    assert(b.variableName == "B")

    val alt = b.next.asAlt
    assert(alt.greedy)
    assert(alt.loopPath == a)
    assert(alt.exitPath == AcceptNode)
  }

  test("compile - grouped pattern (A B)+ C") {
    val group = QuantifiedPattern(
      PatternSequence(Seq(PatternVariable("A"), PatternVariable("B"))),
      OneOrMore, greedy = true)
    val pattern = PatternSequence(Seq(group, PatternVariable("C")))
    val sm = compilePattern(pattern)

    // Structure: A -> B -> AltNode(loop to A, exit to C) -> C -> Accept
    val a = sm.startNode.asMatch
    assert(a.variableName == "A")

    val b = a.next.asMatch

    val alt = b.next.asAlt
    assert(alt.loopPath == a)

    val c = alt.exitPath.asMatch
    assert(c.variableName == "C")
    assert(c.next == AcceptNode)
  }

  test("compile - reluctant grouped pattern (A B)+?") {
    val inner = PatternSequence(Seq(PatternVariable("A"), PatternVariable("B")))
    val pattern = QuantifiedPattern(inner, OneOrMore, greedy = false)
    val sm = compilePattern(pattern)

    val a = sm.startNode.asMatch
    val b = a.next.asMatch
    val alt = b.next.asAlt

    assert(!alt.greedy)
    assert(alt.loopPath == a)
    assert(alt.exitPath == AcceptNode)
  }

  test("compile - nested groups ((A B)+ C)+") {
    // Inner group: (A B)+
    val innerGroup = QuantifiedPattern(
      PatternSequence(Seq(PatternVariable("A"), PatternVariable("B"))),
      OneOrMore, greedy = true)
    // Outer group: ((A B)+ C)+
    val outerGroup = QuantifiedPattern(
      PatternSequence(Seq(innerGroup, PatternVariable("C"))),
      OneOrMore, greedy = true)
    val sm = compilePattern(outerGroup)

    // Structure:
    //   A -> B -> AltNode1(loop to A, exit to C)
    //             -> C -> AltNode2(loop to A, exit to Accept)
    val a = sm.startNode.asMatch
    assert(a.variableName == "A")

    val b = a.next.asMatch
    assert(b.variableName == "B")

    val innerAlt = b.next.asAlt
    assert(innerAlt.greedy)
    assert(innerAlt.loopPath == a) // inner loop

    val c = innerAlt.exitPath.asMatch
    assert(c.variableName == "C")

    val outerAlt = c.next.asAlt
    assert(outerAlt.greedy)
    assert(outerAlt.loopPath == a) // outer loop
    assert(outerAlt.exitPath == AcceptNode)
  }

  // ==================== Transitions and canComplete Tests ====================
  //
  // These tests verify both transition behavior and canComplete logic for each pattern.
  // Key insights:
  // - canComplete returns true only if pattern can complete WITHOUT consuming more rows
  // - At group START boundary, canComplete returns false (must try matching first;
  //   completed states in MatchingEngine's activeStates handle returning completed matches)
  // - At group END boundary, multiple transitions are returned (loop and exit options)

  test("A - transitions") {
    // For simple pattern A, matching A immediately completes the pattern.
    // No intermediate state exists to test canComplete on.
    val pattern = PatternVariable("A")
    val sm = compilePatternWithPredicates(pattern, Map("A" -> 1))
    val a = sm.startNode.asMatch

    // === At start ===
    val atA = NFAState(a)
    assert(!sm.canComplete(atA), "At start - cannot complete (need A)")

    // === Matching A completes immediately ===
    val transitions = sm.getAllTransitions(atA, createRow(1))
    assert(transitions.size == 1, s"Expected 1 transition")
    assert(transitions(0).isInstanceOf[PatternComplete])
    assert(transitions(0).asInstanceOf[PatternComplete].matchedVariable == Some("A"))
  }

  test("A B - transitions") {
    // For sequence A B, matching A advances to B, matching B completes immediately.
    val pattern = PatternSequence(Seq(PatternVariable("A"), PatternVariable("B")))
    val sm = compilePatternWithPredicates(pattern, Map("A" -> 1, "B" -> 2))
    val a = sm.startNode.asMatch
    val b = a.next.asMatch

    // === At start ===
    val initState = NFAState(a)
    assert(!sm.canComplete(initState), "At start - cannot complete")

    // === Transitions at A: advance to B ===
    val transitionsAtA = sm.getAllTransitions(initState, createRow(1))
    assert(transitionsAtA.size == 1, s"Expected 1 transition at A")
    val advanceToB = transitionsAtA(0).asInstanceOf[MatchedTransition]
    assert(advanceToB.state.node == b, "Should advance to B")
    assert(!sm.canComplete(advanceToB.state), "At B - cannot complete (B not matched)")

    // === Transitions at B: complete ===
    val transitionsAtB = sm.getAllTransitions(advanceToB.state, createRow(2))
    assert(transitionsAtB.size == 1, s"Expected 1 transition at B")
    assert(transitionsAtB(0).isInstanceOf[PatternComplete])
  }

  test("A+ greedy - transitions and canComplete") {
    val pattern = QuantifiedPattern(PatternVariable("A"), OneOrMore, greedy = true)
    val sm = compilePatternWithPredicates(pattern, Map("A" -> 1))
    val a = sm.startNode.asMatch
    val initState = NFAState(a)
    val row = createRow(1)

    // === At start ===
    assert(!sm.canComplete(initState), "At start - cannot complete (need at least one A)")

    // === First match ===
    // After matching, count=1 satisfies min=1, so both stay and complete valid
    val transitions = sm.getAllTransitions(initState, row)
    assert(transitions.size == 2, s"Expected 2 transitions, got ${transitions.size}")
    // Greedy: MatchedTransition (stay) comes FIRST, PatternComplete comes second
    assert(transitions(0).isInstanceOf[MatchedTransition], "Greedy should prefer stay first")
    val stay = transitions(0).asInstanceOf[MatchedTransition]
    assert(stay.matchedVariable == "A")
    assert(stay.state.node == a) // stays at same node
    assert(transitions(1).isInstanceOf[PatternComplete], "Complete should be second option")

    // After first match, can complete
    assert(sm.canComplete(stay.state), "After first match - can complete")

    // === After multiple matches ===
    val afterTwo = initState.incrementCount(a.id).incrementCount(a.id)
    assert(sm.canComplete(afterTwo), "After two matches - can complete")
    val transitions2 = sm.getAllTransitions(afterTwo, row)
    assert(transitions2.size == 2, s"Expected 2 transitions, got ${transitions2.size}")
    assert(transitions2(0).isInstanceOf[MatchedTransition], "Greedy should still prefer stay")
    assert(transitions2(1).isInstanceOf[PatternComplete])
  }

  test("A+? reluctant - transitions and canComplete") {
    val pattern = QuantifiedPattern(PatternVariable("A"), OneOrMore, greedy = false)
    val sm = compilePatternWithPredicates(pattern, Map("A" -> 1))
    val a = sm.startNode.asMatch
    val row = createRow(1)
    val initState = NFAState(a)

    // === At start ===
    assert(!sm.canComplete(initState), "At start - cannot complete (need at least one A)")

    // === First match ===
    // After matching, count=1 satisfies min=1, so both stay and complete valid
    val transitions = sm.getAllTransitions(initState, row)
    assert(transitions.size == 2, s"Expected 2 transitions, got ${transitions.size}")
    // Reluctant: PatternComplete comes FIRST, MatchedTransition (stay) comes second
    assert(transitions(0).isInstanceOf[PatternComplete], "Reluctant should prefer complete first")
    val complete = transitions(0).asInstanceOf[PatternComplete]
    assert(complete.matchedVariable == Some("A"))
    assert(transitions(1).isInstanceOf[MatchedTransition], "Stay should be second option")

    // After first match, can complete
    val stay = transitions(1).asInstanceOf[MatchedTransition]
    assert(sm.canComplete(stay.state), "After first match - can complete")

    // === After multiple matches ===
    val afterTwo = initState.incrementCount(a.id).incrementCount(a.id)
    assert(sm.canComplete(afterTwo), "After two matches - can complete")
    val transitions2 = sm.getAllTransitions(afterTwo, row)
    assert(transitions2.size == 2, s"Expected 2 transitions, got ${transitions2.size}")
    assert(transitions2(0).isInstanceOf[PatternComplete], "Reluctant should still prefer complete")
    assert(transitions2(1).isInstanceOf[MatchedTransition])
  }

  test("A+ B - transitions and canComplete") {
    // A+ B pattern where A matches 1, B matches 2
    val pattern = PatternSequence(Seq(
      QuantifiedPattern(PatternVariable("A"), OneOrMore, greedy = true),
      PatternVariable("B")
    ))
    val sm = compilePatternWithPredicates(pattern, Map("A" -> 1, "B" -> 2))

    val a = sm.startNode.asMatch
    val b = a.next.asMatch

    // === At start ===
    assert(!sm.canComplete(NFAState(a)), "At start - cannot complete (need A+ then B)")

    // === Transitions at A (matching row): greedy stay or advance to B ===
    val initState = NFAState(a)
    val transitionsAtA = sm.getAllTransitions(initState, createRow(1))
    assert(transitionsAtA.size == 2, s"Expected 2 transitions at A")
    // Greedy: stay at A first, then advance to B
    val stayAtA = transitionsAtA(0).asInstanceOf[MatchedTransition]
    val advanceToB = transitionsAtA(1).asInstanceOf[MatchedTransition]
    assert(stayAtA.state.node == a, "First transition stays at A")
    assert(advanceToB.state.node == b, "Second transition advances to B")
    // Neither state can complete yet
    assert(!sm.canComplete(stayAtA.state), "State at A cannot complete (need B)")
    assert(!sm.canComplete(advanceToB.state), "State at B cannot complete (B not matched)")

    // === Transitions at B (matching row): complete ===
    val transitionsAtB = sm.getAllTransitions(advanceToB.state, createRow(2))
    assert(transitionsAtB.size == 1, s"Expected 1 transition at B")
    assert(transitionsAtB(0).isInstanceOf[PatternComplete])
    assert(transitionsAtB(0).asInstanceOf[PatternComplete].matchedVariable == Some("B"))

    // === Transitions at A with non-matching row ===
    // Since min is satisfied (count=1), can advance to B and complete
    val transitionsAtANonMatch = sm.getAllTransitions(stayAtA.state, createRow(2))
    assert(transitionsAtANonMatch.size == 1, s"Expected 1 transition")
    assert(transitionsAtANonMatch(0).isInstanceOf[PatternComplete])
  }

  test("transitions - non-matching row with min not satisfied is dead end") {
    val pattern = QuantifiedPattern(PatternVariable("A"), AtLeast(2), greedy = true)
    val sm = compilePatternWithPredicates(pattern, Map("A" -> 1))
    val row = createRow(2) // doesn't match

    // At A with only 1 match (min=2)
    val afterOne = NFAState(sm.startNode).incrementCount(sm.startNode.asMatch.id)
    val transitions = sm.getAllTransitions(afterOne, row)

    // Should be empty - no valid transitions when min not satisfied
    assert(transitions.isEmpty, s"Expected empty transitions, got ${transitions.size}")
  }

  // ==================== Grouped Pattern Tests ====================

  test("(A B)+ greedy - transitions and canComplete") {
    // (A B)+ pattern where A matches 1, B matches 2
    val inner = PatternSequence(Seq(PatternVariable("A"), PatternVariable("B")))
    val pattern = QuantifiedPattern(inner, OneOrMore, greedy = true)
    val sm = compilePatternWithPredicates(pattern, Map("A" -> 1, "B" -> 2))

    val a = sm.startNode.asMatch
    val b = a.next.asMatch

    // === At start ===
    val initState = NFAState(a)
    assert(!sm.canComplete(initState), "At start - cannot complete (need AB)")

    // === Transitions at A: advance to B ===
    val transitionsAtA = sm.getAllTransitions(initState, createRow(1))
    assert(transitionsAtA.size == 1, s"Expected 1 transition at A")
    val advanceToB = transitionsAtA(0).asInstanceOf[MatchedTransition]
    assert(advanceToB.state.node == b, "Should advance to B")
    assert(!sm.canComplete(advanceToB.state), "At B - cannot complete (B not matched)")

    // === Transitions at B: 2 options (greedy loop, complete) ===
    val transitionsAtB = sm.getAllTransitions(advanceToB.state, createRow(2))
    assert(transitionsAtB.size == 2, s"Expected 2 transitions at B")
    // Greedy: loop (back to A) comes FIRST, then exit (complete)
    val loopToA = transitionsAtB(0).asInstanceOf[MatchedTransition]
    assert(loopToA.state.node == a, "First option loops back to A")
    assert(loopToA.matchedVariable == "B", "Should record B as matched variable")
    assert(transitionsAtB(1).isInstanceOf[PatternComplete], "Second option completes")

    // State at A (after loop back) cannot complete - at group START
    assert(!sm.canComplete(loopToA.state), "At group START, canComplete returns false")
  }

  test("(A B)+? reluctant - transitions and canComplete") {
    // (A B)+? pattern where A matches 1, B matches 2
    val inner = PatternSequence(Seq(PatternVariable("A"), PatternVariable("B")))
    val pattern = QuantifiedPattern(inner, OneOrMore, greedy = false)
    val sm = compilePatternWithPredicates(pattern, Map("A" -> 1, "B" -> 2))

    val a = sm.startNode.asMatch
    val b = a.next.asMatch

    // === At start ===
    val initState = NFAState(a)
    assert(!sm.canComplete(initState), "At start - cannot complete (need AB)")

    // === Transitions at A: advance to B ===
    val transitionsAtA = sm.getAllTransitions(initState, createRow(1))
    assert(transitionsAtA.size == 1, s"Expected 1 transition at A")
    val advanceToB = transitionsAtA(0).asInstanceOf[MatchedTransition]
    assert(advanceToB.state.node == b, "Should advance to B")
    assert(!sm.canComplete(advanceToB.state), "At B - cannot complete (B not matched)")

    // === Transitions at B: 2 options (reluctant prefers complete first) ===
    val transitionsAtB = sm.getAllTransitions(advanceToB.state, createRow(2))
    assert(transitionsAtB.size == 2, s"Expected 2 transitions at B")
    // Reluctant: exit (complete) comes FIRST, then loop (back to A)
    assert(transitionsAtB(0).isInstanceOf[PatternComplete], "First option completes")
    assert(transitionsAtB(0).asInstanceOf[PatternComplete].matchedVariable == Some("B"))
    val loopToA = transitionsAtB(1).asInstanceOf[MatchedTransition]
    assert(loopToA.state.node == a, "Second option loops back to A")
  }

  test("(A B)+ C - transitions and canComplete") {
    val group = QuantifiedPattern(
      PatternSequence(Seq(PatternVariable("A"), PatternVariable("B"))),
      OneOrMore, greedy = true)
    val pattern = PatternSequence(Seq(group, PatternVariable("C")))
    val sm = compilePatternWithPredicates(pattern, Map("A" -> 1, "B" -> 2, "C" -> 3))

    val a = sm.startNode.asMatch
    val b = a.next.asMatch
    val alt = b.next.asAlt
    val c = alt.exitPath.asMatch

    // === At start ===
    val initState = NFAState(a)
    assert(!sm.canComplete(initState), "At start - cannot complete")

    // === Transitions at A: advance to B ===
    val transitionsAtA = sm.getAllTransitions(initState, createRow(1))
    assert(transitionsAtA.size == 1, s"Expected 1 transition at A")
    val advanceToB = transitionsAtA(0).asInstanceOf[MatchedTransition]
    assert(advanceToB.state.node == b, "Should advance to B")
    assert(!sm.canComplete(advanceToB.state), "At B - cannot complete (need B and C)")

    // === Transitions at B: 2 options (greedy loop, exit to C) ===
    val transitionsAtB = sm.getAllTransitions(advanceToB.state, createRow(2))
    assert(transitionsAtB.size == 2, s"Expected 2 transitions at B")
    // Greedy: loop (to A) first, then exit (to C)
    val loopToA = transitionsAtB(0).asInstanceOf[MatchedTransition]
    val exitToC = transitionsAtB(1).asInstanceOf[MatchedTransition]
    assert(loopToA.state.node == a, "First option loops to A")
    assert(exitToC.state.node == c, "Second option exits to C")
    assert(!sm.canComplete(loopToA.state), "State at A cannot complete (need C)")
    assert(!sm.canComplete(exitToC.state), "State at C cannot complete (C not matched)")

    // === Transitions at C: complete ===
    val transitionsAtC = sm.getAllTransitions(exitToC.state, createRow(3))
    assert(transitionsAtC.size == 1, s"Expected 1 transition at C")
    assert(transitionsAtC(0).isInstanceOf[PatternComplete])
  }

  test("((A B)+ C)+ - transitions and canComplete") {
    // Pattern: ((A B)+ C)+ where A=1, B=2, C=3
    // Group boundaries:
    //   - A: inner group START
    //   - B: inner group END (multiple transitions: loop to A, or exit to C)
    //   - C: outer group END (multiple transitions: loop to A, or complete)
    val innerGroup = QuantifiedPattern(
      PatternSequence(Seq(PatternVariable("A"), PatternVariable("B"))),
      OneOrMore, greedy = true)
    val outerGroup = QuantifiedPattern(
      PatternSequence(Seq(innerGroup, PatternVariable("C"))),
      OneOrMore, greedy = true)
    val sm = compilePatternWithPredicates(outerGroup, Map("A" -> 1, "B" -> 2, "C" -> 3))

    val a = sm.startNode.asMatch
    val b = a.next.asMatch
    val innerAlt = b.next.asAlt
    val c = innerAlt.exitPath.asMatch

    // === At start ===
    val initState = NFAState(a)
    assert(!sm.canComplete(initState), "At start - cannot complete")

    // === Transitions at A: advance to B ===
    val transitionsAtA = sm.getAllTransitions(initState, createRow(1))
    assert(transitionsAtA.size == 1, s"Expected 1 transition at A")
    val advanceToB = transitionsAtA(0).asInstanceOf[MatchedTransition]
    assert(advanceToB.state.node == b, "Should advance to B")
    assert(!sm.canComplete(advanceToB.state), "At B - cannot complete (need B and C)")

    // === Transitions at B: 2 options (greedy inner loop, exit to C) ===
    val transitionsAtB = sm.getAllTransitions(advanceToB.state, createRow(2))
    assert(transitionsAtB.size == 2, s"Expected 2 transitions at B")
    // Greedy inner: loop (to A) first, then exit (to C)
    val innerLoopToA = transitionsAtB(0).asInstanceOf[MatchedTransition]
    val exitToC = transitionsAtB(1).asInstanceOf[MatchedTransition]
    assert(innerLoopToA.state.node == a, "First option loops to A")
    assert(exitToC.state.node == c, "Second option exits to C")
    assert(!sm.canComplete(innerLoopToA.state), "State at A cannot complete (need C)")
    assert(!sm.canComplete(exitToC.state), "State at C cannot complete (C not matched)")

    // === Transitions at A (inner loop): advance to B ===
    val transitionsAtA_innerLoop = sm.getAllTransitions(innerLoopToA.state, createRow(1))
    assert(transitionsAtA_innerLoop.size == 1, s"Expected 1 transition at A (inner loop)")
    val advanceToB2 = transitionsAtA_innerLoop(0).asInstanceOf[MatchedTransition]
    assert(advanceToB2.state.node == b, "Should advance to B")

    // A (inner loop) with non-matching row - dead end
    val transitionsAtA_innerLoopNoMatch = sm.getAllTransitions(innerLoopToA.state, createRow(999))
    assert(transitionsAtA_innerLoopNoMatch.isEmpty, "A with non-matching row is dead end")

    // === Transitions at C: 2 options (greedy outer loop, complete) ===
    val transitionsAtC = sm.getAllTransitions(exitToC.state, createRow(3))
    assert(transitionsAtC.size == 2, s"Expected 2 transitions at C")
    // Greedy outer: loop (back to A) first, then complete
    val outerLoopToA = transitionsAtC(0).asInstanceOf[MatchedTransition]
    assert(outerLoopToA.state.node == a, "First option loops back to A")
    assert(transitionsAtC(1).isInstanceOf[PatternComplete], "Second option completes")

    // State at A (after outer loop) cannot complete - at group START
    assert(!sm.canComplete(outerLoopToA.state), "At group START, canComplete returns false")

    // === Transitions at A (second iteration) ===
    val transitionsAtA2 = sm.getAllTransitions(outerLoopToA.state, createRow(1))
    assert(transitionsAtA2.size == 1, s"Expected 1 transition at A")
    assert(transitionsAtA2(0).asInstanceOf[MatchedTransition].state.node == b)

    // === Transitions at A with non-matching row ===
    // Dead end - match engine handles this via completed states in activeStates
    val transitionsAtANoMatch = sm.getAllTransitions(outerLoopToA.state, createRow(999))
    assert(transitionsAtANoMatch.isEmpty, "A with non-matching row is dead end")
  }

  test("(A (B C)+)+ - transitions and canComplete") {
    // Pattern: (A (B C)+)+ where A=1, B=2, C=3
    // Group boundaries:
    //   - A: outer group START
    //   - B: inner group START
    //   - C: inner AND outer group END (multiple transitions via innerAlt then outerAlt)
    val innerGroup = QuantifiedPattern(
      PatternSequence(Seq(PatternVariable("B"), PatternVariable("C"))),
      OneOrMore, greedy = true)
    val outerGroup = QuantifiedPattern(
      PatternSequence(Seq(PatternVariable("A"), innerGroup)),
      OneOrMore, greedy = true)
    val sm = compilePatternWithPredicates(outerGroup, Map("A" -> 1, "B" -> 2, "C" -> 3))

    val a = sm.startNode.asMatch
    val b = a.next.asMatch
    val c = b.next.asMatch

    // === At start ===
    val initState = NFAState(a)
    assert(!sm.canComplete(initState), "At start - cannot complete")

    // === Transitions at A: advance to B ===
    val transitionsAtA = sm.getAllTransitions(initState, createRow(1))
    assert(transitionsAtA.size == 1, s"Expected 1 transition at A")
    val advanceToB = transitionsAtA(0).asInstanceOf[MatchedTransition]
    assert(advanceToB.state.node == b, "Should advance to B")
    assert(!sm.canComplete(advanceToB.state), "At B - cannot complete (need BC)")

    // === Transitions at B: only advance to C ===
    val transitionsAtB = sm.getAllTransitions(advanceToB.state, createRow(2))
    assert(transitionsAtB.size == 1, s"Expected 1 transition at B")
    val advanceToC = transitionsAtB(0).asInstanceOf[MatchedTransition]
    assert(advanceToC.state.node == c, "Should advance to C")
    assert(!sm.canComplete(advanceToC.state), "At C - cannot complete (C not matched)")

    // B with non-matching row - dead end
    val transitionsAtBNoMatch = sm.getAllTransitions(advanceToB.state, createRow(999))
    assert(transitionsAtBNoMatch.isEmpty, "B with non-matching row is dead end")

    // === Transitions at C: 3 options (greedy inner loop, greedy outer loop, complete) ===
    val transitionsAtC = sm.getAllTransitions(advanceToC.state, createRow(3))
    assert(transitionsAtC.size == 3, s"Expected 3 transitions at C")
    // Greedy inner: loop (to B) first
    val innerLoopToB = transitionsAtC(0).asInstanceOf[MatchedTransition]
    assert(innerLoopToB.state.node == b, "First option loops to B")
    // Greedy outer: loop (to A) second
    val outerLoopToA = transitionsAtC(1).asInstanceOf[MatchedTransition]
    assert(outerLoopToA.state.node == a, "Second option loops to A")
    // Complete last
    assert(transitionsAtC(2).isInstanceOf[PatternComplete], "Third option completes")

    // At group START, canComplete returns false
    assert(!sm.canComplete(innerLoopToB.state), "At inner group START (B)")
    assert(!sm.canComplete(outerLoopToA.state), "At outer group START (A)")

    // === Transitions at B (inner loop, second iteration) ===
    val transitionsAtB2 = sm.getAllTransitions(innerLoopToB.state, createRow(2))
    assert(transitionsAtB2.size == 1, s"Expected 1 transition at B (inner loop)")
    val advanceToC2 = transitionsAtB2(0).asInstanceOf[MatchedTransition]
    assert(advanceToC2.state.node == c, "Should advance to C")

    // B with non-matching row from inner loop - dead end
    val transitionsAtB2NoMatch = sm.getAllTransitions(innerLoopToB.state, createRow(999))
    assert(transitionsAtB2NoMatch.isEmpty, "B with non-matching row is dead end")

    // === Transitions at A (second iteration) ===
    val transitionsAtA2 = sm.getAllTransitions(outerLoopToA.state, createRow(1))
    assert(transitionsAtA2.size == 1, s"Expected 1 transition at A")
    assert(transitionsAtA2(0).asInstanceOf[MatchedTransition].state.node == b)

    // A with non-matching row - dead end
    val transitionsAtANoMatch = sm.getAllTransitions(outerLoopToA.state, createRow(999))
    assert(transitionsAtANoMatch.isEmpty, "A with non-matching row is dead end")
  }
}
