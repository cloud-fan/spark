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
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  AttributeReference,
  EqualTo,
  GenericInternalRow,
  GreaterThanOrEqual,
  LessThanOrEqual,
  Literal,
  UnsafeProjection
}
import org.apache.spark.sql.catalyst.plans.logical.{
  OneOrMore,
  PatternSequence,
  PatternVariable,
  QuantifiedPattern,
  RowPattern
}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
 * Unit tests for MatchingEngine with graph-based NFA design.
 */
class MatchingEngineSuite extends SparkFunSuite {

  private val schema = StructType(Seq(
    StructField("value", IntegerType, nullable = false)
  ))

  private val toUnsafe = UnsafeProjection.create(schema)

  // Shared attribute for all pattern compilations
  private val valueAttr = AttributeReference("value", IntegerType)()

  private def createRow(value: Int): org.apache.spark.sql.catalyst.expressions.UnsafeRow = {
    val row = new GenericInternalRow(Array[Any](value))
    toUnsafe.apply(row).copy()
  }

  /** Shorthand for creating MatchedPattern in test assertions */
  private def M(varName: String, count: Int): MatchedPattern = MatchedPattern(varName, count)

  /**
   * Compiles a pattern with predicates defined by expressions.
   * @param pattern The row pattern to compile
   * @param varToExpr Map from variable name to the expression (using valueAttr)
   */
  private def compilePattern(
      pattern: RowPattern,
      varToExpr: Map[String, org.apache.spark.sql.catalyst.expressions.Expression])
      : PatternStateMachine = {
    val varNames = pattern.variableNames.toSeq
    val definitions = varNames.map { name =>
      val expr = varToExpr.getOrElse(name, Literal(true))
      Alias(expr, name)()
    }
    PatternStateMachine.compile(pattern, definitions, Seq(valueAttr), partitionIndex = 0)
  }

  // ==================== Pattern Creation Helpers ====================

  /** Pattern "A+" where A matches value == 1 */
  private def createAPlusPattern(): PatternStateMachine = {
    val pattern = QuantifiedPattern(PatternVariable("A"), OneOrMore, greedy = true)
    compilePattern(pattern, Map("A" -> EqualTo(valueAttr, Literal(1))))
  }

  /** Pattern "A+?" (reluctant) where A matches value == 1 */
  private def createAReluctantPlusPattern(): PatternStateMachine = {
    val pattern = QuantifiedPattern(PatternVariable("A"), OneOrMore, greedy = false)
    compilePattern(pattern, Map("A" -> EqualTo(valueAttr, Literal(1))))
  }

  /** Pattern "A+ B" where A matches 1, B matches 2 */
  private def createAPlusBPattern(): PatternStateMachine = {
    val pattern = PatternSequence(Seq(
      QuantifiedPattern(PatternVariable("A"), OneOrMore, greedy = true),
      PatternVariable("B")
    ))
    compilePattern(pattern, Map(
      "A" -> EqualTo(valueAttr, Literal(1)),
      "B" -> EqualTo(valueAttr, Literal(2))
    ))
  }

  /** Pattern "A+ B+" (greedy) with overlapping predicates: A <= 2, B >= 2 */
  private def createGreedyAPlusBPlusPattern(): PatternStateMachine = {
    val pattern = PatternSequence(Seq(
      QuantifiedPattern(PatternVariable("A"), OneOrMore, greedy = true),
      QuantifiedPattern(PatternVariable("B"), OneOrMore, greedy = true)
    ))
    compilePattern(pattern, Map(
      "A" -> LessThanOrEqual(valueAttr, Literal(2)),
      "B" -> GreaterThanOrEqual(valueAttr, Literal(2))
    ))
  }

  /** Pattern "A+? B+" (reluctant A) with overlapping predicates: A <= 2, B >= 2 */
  private def createReluctantAPlusBPlusPattern(): PatternStateMachine = {
    val pattern = PatternSequence(Seq(
      QuantifiedPattern(PatternVariable("A"), OneOrMore, greedy = false),
      QuantifiedPattern(PatternVariable("B"), OneOrMore, greedy = true)
    ))
    compilePattern(pattern, Map(
      "A" -> LessThanOrEqual(valueAttr, Literal(2)),
      "B" -> GreaterThanOrEqual(valueAttr, Literal(2))
    ))
  }

  /** Pattern "(A B)+" where A=1, B=2 */
  private def createGroupedABPlusPattern(): PatternStateMachine = {
    val inner = PatternSequence(Seq(PatternVariable("A"), PatternVariable("B")))
    val pattern = QuantifiedPattern(inner, OneOrMore, greedy = true)
    compilePattern(pattern, Map(
      "A" -> EqualTo(valueAttr, Literal(1)),
      "B" -> EqualTo(valueAttr, Literal(2))
    ))
  }

  /** Pattern "(A B)+?" (reluctant) where A=1, B=2 */
  private def createReluctantGroupedABPattern(): PatternStateMachine = {
    val inner = PatternSequence(Seq(PatternVariable("A"), PatternVariable("B")))
    val pattern = QuantifiedPattern(inner, OneOrMore, greedy = false)
    compilePattern(pattern, Map(
      "A" -> EqualTo(valueAttr, Literal(1)),
      "B" -> EqualTo(valueAttr, Literal(2))
    ))
  }

  /** Pattern "(A B)+ C" where A=1, B=2, C=3 */
  private def createGroupedABPlusCPattern(): PatternStateMachine = {
    val group = QuantifiedPattern(
      PatternSequence(Seq(PatternVariable("A"), PatternVariable("B"))),
      OneOrMore, greedy = true)
    val pattern = PatternSequence(Seq(group, PatternVariable("C")))
    compilePattern(pattern, Map(
      "A" -> EqualTo(valueAttr, Literal(1)),
      "B" -> EqualTo(valueAttr, Literal(2)),
      "C" -> EqualTo(valueAttr, Literal(3))
    ))
  }

  /** Pattern "(A (B C)+)+" where A=1, B=2, C=3 */
  private def createABCNestedPattern(): PatternStateMachine = {
    val innerGroup = QuantifiedPattern(
      PatternSequence(Seq(PatternVariable("B"), PatternVariable("C"))),
      OneOrMore, greedy = true)
    val outerGroup = QuantifiedPattern(
      PatternSequence(Seq(PatternVariable("A"), innerGroup)),
      OneOrMore, greedy = true)
    compilePattern(outerGroup, Map(
      "A" -> EqualTo(valueAttr, Literal(1)),
      "B" -> EqualTo(valueAttr, Literal(2)),
      "C" -> EqualTo(valueAttr, Literal(3))
    ))
  }

  /** Pattern "((A B)+ C)+" where A=1, B=2, C=3 */
  private def createNestedGroupPattern(): PatternStateMachine = {
    val innerGroup = QuantifiedPattern(
      PatternSequence(Seq(PatternVariable("A"), PatternVariable("B"))),
      OneOrMore, greedy = true)
    val outerGroup = QuantifiedPattern(
      PatternSequence(Seq(innerGroup, PatternVariable("C"))),
      OneOrMore, greedy = true)
    compilePattern(outerGroup, Map(
      "A" -> EqualTo(valueAttr, Literal(1)),
      "B" -> EqualTo(valueAttr, Literal(2)),
      "C" -> EqualTo(valueAttr, Literal(3))
    ))
  }

  // ==================== Basic Tests ====================

  test("A+ pattern with single A - endOfInput completes") {
    val engine = new MatchingEngine(createAPlusPattern())

    val result1 = engine.processRow(createRow(1))
    assert(result1.isEmpty, "Should not complete on first row without endOfInput")

    val result2 = engine.endOfInput()
    assert(result2.isDefined, "Should complete on endOfInput")
    assert(result2.get.startOffset == 0)
    assert(result2.get.getMatchHistory == Seq(M("A", 1)))
  }

  test("A+ pattern with multiple A's - endOfInput completes with all") {
    val engine = new MatchingEngine(createAPlusPattern())

    engine.processRow(createRow(1))
    engine.processRow(createRow(1))
    engine.processRow(createRow(1))

    val result = engine.endOfInput()
    assert(result.isDefined)
    assert(result.get.getMatchHistory == Seq(M("A", 3)), "Should match all 3 A's (greedy)")
  }

  test("A+ pattern - no match when first row doesn't match") {
    val engine = new MatchingEngine(createAPlusPattern())

    val result = engine.processRow(createRow(2))
    assert(result.isEmpty)

    val endResult = engine.endOfInput()
    assert(endResult.isEmpty, "Should not complete when pattern never started")
  }

  test("A+ pattern - first row matches but second row doesn't") {
    val engine = new MatchingEngine(createAPlusPattern())

    // First row matches A (value 1 <= 1)
    val r1 = engine.processRow(createRow(1))
    assert(r1.isEmpty, "Should not complete yet, pattern still open")

    // Second row doesn't match A (value 2 > 1)
    // Since A+ min is satisfied, pattern can complete
    val r2 = engine.processRow(createRow(2))
    assert(r2.isDefined, "Should complete when non-matching row arrives after min satisfied")
    assert(r2.get.getMatchHistory == Seq(M("A", 1)))
  }

  test("A+ B pattern - completes when B arrives") {
    val engine = new MatchingEngine(createAPlusBPattern())

    engine.processRow(createRow(1))
    engine.processRow(createRow(1))

    val result = engine.processRow(createRow(2))
    assert(result.isDefined, "Should complete when B arrives")
    assert(result.get.getMatchHistory == Seq(M("A", 2), M("B", 1)))
  }

  test("A+ B pattern - no complete on endOfInput without B") {
    val engine = new MatchingEngine(createAPlusBPattern())

    engine.processRow(createRow(1))
    engine.processRow(createRow(1))

    val result = engine.endOfInput()
    assert(result.isEmpty, "Should not complete without B")
  }

  test("greedy A+ B+ with overlapping predicates") {
    // Pattern: A+ B+ where A matches <=2, B matches >=2
    // Input: 1, 2, 2, 3
    // Greedy A takes all rows matching <=2: values 1, 2, 2
    // Row 4 (value=3): A can't match (3 > 2), advances to B. B matches (3 >= 2)
    // B+ is greedy so it continues, pattern completes on endOfInput
    val engine = new MatchingEngine(createGreedyAPlusBPlusPattern())

    engine.processRow(createRow(1))
    engine.processRow(createRow(2))
    engine.processRow(createRow(2))
    engine.processRow(createRow(3))

    val result = engine.endOfInput()
    assert(result.isDefined)
    assert(result.get.getMatchHistory == Seq(M("A", 3), M("B", 1)))
  }

  test("reluctant A+? B+ with overlapping predicates") {
    // Pattern: A+? B+ where A matches <=2, B matches >=2
    // Input: 1, 2
    // Reluctant A prefers to advance early after min satisfied
    // Row 1 (value=1): matches A (<=2), reluctant advances
    // Row 2 (value=2): matches B (>=2), B+ greedy wants more, completes on endOfInput
    val engine = new MatchingEngine(createReluctantAPlusBPlusPattern())

    engine.processRow(createRow(1))
    engine.processRow(createRow(2))

    val result = engine.endOfInput()
    assert(result.isDefined)
    assert(result.get.getMatchHistory == Seq(M("A", 1), M("B", 1)))
  }

  test("no match - pattern does not start") {
    val engine = new MatchingEngine(createAPlusPattern())

    val result1 = engine.processRow(createRow(2))
    assert(result1.isEmpty)

    val result2 = engine.processRow(createRow(3))
    assert(result2.isEmpty)

    val result3 = engine.endOfInput()
    assert(result3.isEmpty)
  }

  test("reset clears state") {
    val engine = new MatchingEngine(createAPlusPattern())

    engine.processRow(createRow(1))
    engine.reset()

    val result = engine.endOfInput()
    assert(result.isEmpty, "After reset, no active states should remain")
  }

  // ==================== Grouped Pattern Tests ====================

  test("(A B)+ pattern - single AB iteration completes on endOfInput") {
    // After matching A B, greedy prefers to loop (try another AB iteration).
    // The completed state stays in activeStates but matching states come first.
    // When no more input arrives, endOfInput returns the first completable state.
    val engine = new MatchingEngine(createGroupedABPlusPattern())

    engine.processRow(createRow(1))
    engine.processRow(createRow(2))  // Completes one iteration, greedy loops

    val result = engine.endOfInput()  // Returns completed state
    assert(result.isDefined, "Completed state should be returned on endOfInput")
    assert(result.get.getMatchHistory == Seq(M("A", 1), M("B", 1)))
  }

  test("(A B)+ pattern - multiple iterations") {
    val engine = new MatchingEngine(createGroupedABPlusPattern())

    engine.processRow(createRow(1))
    engine.processRow(createRow(2))
    engine.processRow(createRow(1))
    engine.processRow(createRow(2))

    val result = engine.endOfInput()
    assert(result.isDefined)
    // With greedy, 2 iterations: A B A B (run-length encoded)
    assert(result.get.getMatchHistory == Seq(M("A", 1), M("B", 1), M("A", 1), M("B", 1)))
  }

  test("(A B)+ pattern - completes when non-matching row arrives") {
    val engine = new MatchingEngine(createGroupedABPlusPattern())

    engine.processRow(createRow(1))
    engine.processRow(createRow(2))

    // Row 3 doesn't match A, so pattern should complete
    val result = engine.processRow(createRow(3))
    assert(result.isDefined, "Should complete when non-matching row arrives after full iteration")
    assert(result.get.getMatchHistory == Seq(M("A", 1), M("B", 1)))
  }

  test("(A B)+ pattern - fails if incomplete group at end") {
    val engine = new MatchingEngine(createGroupedABPlusPattern())

    engine.processRow(createRow(1))
    // Only A matched, B not matched

    val result = engine.endOfInput()
    assert(result.isEmpty, "Should not complete with incomplete group")
  }

  test("(A B)+? reluctant pattern - completes after first iteration") {
    val engine = new MatchingEngine(createReluctantGroupedABPattern())

    engine.processRow(createRow(1))

    // Reluctant prefers to exit immediately after first complete iteration
    // So the match completes on processRow, not endOfInput
    val result = engine.processRow(createRow(2))
    assert(result.isDefined, "Reluctant should complete immediately when B matches")
    assert(result.get.getMatchHistory == Seq(M("A", 1), M("B", 1)))
  }

  test("(A B)+ C pattern - greedy loop fails, alternative exit path succeeds") {
    // This test demonstrates: greedy path fails, alternative path matches more rows
    // Pattern: (A B)+ C where A==1, B==2, C==3
    // Input: 1, 2, 3
    // After A B (one iteration), NFA explores both paths:
    //   - Greedy loop path: back to A, waiting for value==1
    //   - Exit path: at C, waiting for value==3
    // Row 3 (value=3):
    //   - Loop path: 3 != 1, A doesn't match, path FAILS
    //   - Exit path: 3 == 3, C matches, pattern COMPLETES
    // The alternative exit path succeeds while the preferred greedy loop path fails.
    val engine = new MatchingEngine(createGroupedABPlusCPattern())

    engine.processRow(createRow(1))  // matches A
    engine.processRow(createRow(2))  // matches B, completes iteration, greedy loops
    val result = engine.processRow(createRow(3))  // loop path fails (A), exit path succeeds (C)

    assert(result.isDefined, "Exit path should succeed when loop path fails")
    assert(result.get.getMatchHistory == Seq(M("A", 1), M("B", 1), M("C", 1)))
  }

  test("(A B)+ C pattern - does not complete at endOfInput without C") {
    val engine = new MatchingEngine(createGroupedABPlusCPattern())

    engine.processRow(createRow(1))
    engine.processRow(createRow(2))

    val result = engine.endOfInput()
    assert(result.isEmpty, "Should not complete without C")
  }

  test("(A B)+ C pattern - non-matching row after group does not complete") {
    val engine = new MatchingEngine(createGroupedABPlusCPattern())

    engine.processRow(createRow(1))
    engine.processRow(createRow(2))

    // Row 4 doesn't match C
    val result = engine.processRow(createRow(4))
    assert(result.isEmpty, "Should not complete when row doesn't match expected C")
  }

  // ==================== Nested Group Tests ====================

  test("((A B)+ C)+ pattern - single outer iteration") {
    val engine = new MatchingEngine(createNestedGroupPattern())

    engine.processRow(createRow(1))
    engine.processRow(createRow(2))
    engine.processRow(createRow(3))

    val result = engine.endOfInput()
    assert(result.isDefined)
    assert(result.get.getMatchHistory == Seq(M("A", 1), M("B", 1), M("C", 1)))
  }

  test("((A B)+ C)+ pattern - multiple inner iterations") {
    val engine = new MatchingEngine(createNestedGroupPattern())

    engine.processRow(createRow(1))
    engine.processRow(createRow(2))
    engine.processRow(createRow(1))
    engine.processRow(createRow(2))
    engine.processRow(createRow(3))

    val result = engine.endOfInput()
    assert(result.isDefined)
    // Inner group has 2 iterations: A B A B C
    assert(result.get.getMatchHistory == Seq(M("A", 1), M("B", 1), M("A", 1), M("B", 1), M("C", 1)))
  }

  test("((A B)+ C)+ pattern - multiple outer iterations") {
    val engine = new MatchingEngine(createNestedGroupPattern())

    // First outer iteration
    engine.processRow(createRow(1))
    engine.processRow(createRow(2))
    engine.processRow(createRow(3))
    // Second outer iteration
    engine.processRow(createRow(1))
    engine.processRow(createRow(2))
    engine.processRow(createRow(3))

    val result = engine.endOfInput()
    assert(result.isDefined)
    // Two outer iterations: A B C A B C
    assert(result.get.getMatchHistory ==
      Seq(M("A", 1), M("B", 1), M("C", 1), M("A", 1), M("B", 1), M("C", 1)))
  }

  test("((A B)+ C)+ pattern - completes when non-matching row arrives") {
    val engine = new MatchingEngine(createNestedGroupPattern())

    // Complete one outer iteration
    engine.processRow(createRow(1))  // A
    engine.processRow(createRow(2))  // B
    engine.processRow(createRow(3))  // C

    // Non-matching row (4) causes matching states to die, completed state becomes first
    val result = engine.processRow(createRow(4))
    assert(result.isDefined, "Should return completed state when matching states die")
    assert(result.get.getMatchHistory == Seq(M("A", 1), M("B", 1), M("C", 1)))
  }

  test("(A (B C)+)+ pattern - single outer iteration") {
    val engine = new MatchingEngine(createABCNestedPattern())

    engine.processRow(createRow(1))
    engine.processRow(createRow(2))
    engine.processRow(createRow(3))

    val result = engine.endOfInput()
    assert(result.isDefined)
    assert(result.get.getMatchHistory == Seq(M("A", 1), M("B", 1), M("C", 1)))
  }

  test("(A (B C)+)+ pattern - multiple inner iterations") {
    val engine = new MatchingEngine(createABCNestedPattern())

    engine.processRow(createRow(1))
    engine.processRow(createRow(2))
    engine.processRow(createRow(3))
    engine.processRow(createRow(2))
    engine.processRow(createRow(3))

    val result = engine.endOfInput()
    assert(result.isDefined)
    // Inner (B C)+ has 2 iterations: A B C B C
    assert(result.get.getMatchHistory == Seq(M("A", 1), M("B", 1), M("C", 1), M("B", 1), M("C", 1)))
  }

  test("(A (B C)+)+ pattern - multiple outer iterations") {
    val engine = new MatchingEngine(createABCNestedPattern())

    // First outer: A BC
    engine.processRow(createRow(1))
    engine.processRow(createRow(2))
    engine.processRow(createRow(3))
    // Second outer: A BC BC
    engine.processRow(createRow(1))
    engine.processRow(createRow(2))
    engine.processRow(createRow(3))
    engine.processRow(createRow(2))
    engine.processRow(createRow(3))

    val result = engine.endOfInput()
    assert(result.isDefined)
    // First outer: A BC, Second outer: A BC BC
    assert(result.get.getMatchHistory ==
      Seq(M("A", 1), M("B", 1), M("C", 1), M("A", 1), M("B", 1), M("C", 1), M("B", 1), M("C", 1)))
  }

  // ==================== Nested Group Negative Tests ====================

  test("((A B)+ C)+ pattern - fails with incomplete inner group") {
    val engine = new MatchingEngine(createNestedGroupPattern())

    engine.processRow(createRow(1))  // A only, no B

    val result = engine.endOfInput()
    assert(result.isEmpty, "Should not complete with incomplete inner group (A B)+")
  }

  test("((A B)+ C)+ pattern - fails without C") {
    val engine = new MatchingEngine(createNestedGroupPattern())

    engine.processRow(createRow(1))
    engine.processRow(createRow(2))  // Complete AB, but no C

    val result = engine.endOfInput()
    assert(result.isEmpty, "Should not complete without C")
  }

  test("((A B)+ C)+ pattern - fails with incomplete second outer iteration (A only)") {
    val engine = new MatchingEngine(createNestedGroupPattern())

    // First complete iteration
    engine.processRow(createRow(1))
    engine.processRow(createRow(2))
    engine.processRow(createRow(3))
    // Incomplete second iteration - only A
    engine.processRow(createRow(1))

    val result = engine.endOfInput()
    // Should return the first complete iteration (completed state in activeStates)
    assert(result.isDefined, "Should return completed state from first iteration")
    assert(result.get.getMatchHistory == Seq(M("A", 1), M("B", 1), M("C", 1)))
  }

  test("((A B)+ C)+ pattern - fails with incomplete second outer iteration (AB then D)") {
    val engine = new MatchingEngine(createNestedGroupPattern())

    // First complete iteration: A B C
    engine.processRow(createRow(1))
    engine.processRow(createRow(2))
    engine.processRow(createRow(3))
    // Second iteration: A B, then D (doesn't match C)
    engine.processRow(createRow(1))
    engine.processRow(createRow(2))
    // D=4 doesn't match C=3, matching states die, completed state becomes first
    val result = engine.processRow(createRow(4))
    // Should return the first complete iteration (completed state)
    // The second iteration AB is incomplete because D doesn't match C
    assert(result.isDefined, "Should return completed state from first iteration")
    assert(result.get.getMatchHistory == Seq(M("A", 1), M("B", 1), M("C", 1)))
  }

  test("(A (B C)+)+ pattern - fails with A only") {
    val engine = new MatchingEngine(createABCNestedPattern())

    engine.processRow(createRow(1))  // A only, no B C

    val result = engine.endOfInput()
    assert(result.isEmpty, "Should not complete with only A, missing (B C)+")
  }

  test("(A (B C)+)+ pattern - fails with incomplete inner group") {
    val engine = new MatchingEngine(createABCNestedPattern())

    engine.processRow(createRow(1))
    engine.processRow(createRow(2))  // A B only, no C

    val result = engine.endOfInput()
    assert(result.isEmpty, "Should not complete with incomplete inner group (B C)+")
  }
}
