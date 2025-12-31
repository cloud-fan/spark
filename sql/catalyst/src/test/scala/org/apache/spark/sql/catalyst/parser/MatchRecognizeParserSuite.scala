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

package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.logical._

class MatchRecognizeParserSuite extends AnalysisTest {

  import CatalystSqlParser._
  import org.apache.spark.sql.catalyst.dsl.expressions._
  import org.apache.spark.sql.catalyst.dsl.plans._

  private def assertEqual(sqlCommand: String, plan: LogicalPlan): Unit = {
    comparePlans(parsePlan(sqlCommand), plan, checkAnalysis = false)
  }

  private def intercept(sqlCommand: String, condition: Option[String], messages: String*): Unit =
    interceptParseException(parsePlan)(sqlCommand, messages: _*)(condition)

  test("match_recognize - basic pattern") {
    assertEqual(
      """
        |SELECT * FROM t
        |MATCH_RECOGNIZE (
        |  PATTERN (A B)
        |  DEFINE A AS price > 100
        |)
        |""".stripMargin,
      UnresolvedMatchRecognize(
        partitionSpec = Seq.empty,
        orderSpec = Seq.empty,
        pattern = PatternSequence(Seq(PatternVariable("a"), PatternVariable("b"))),
        patternVariableDefinitions = Seq(($"price" > 100).as("a")),
        measures = Seq.empty,
        allRowsPerMatch = false,
        virtualColumns = MatchRecognizeVirtualColumns.empty,
        table("t")
      ).select(star())
    )
  }

  test("match_recognize - with partition and order") {
    assertEqual(
      """
        |SELECT * FROM t
        |MATCH_RECOGNIZE (
        |  PARTITION BY symbol
        |  ORDER BY ts
        |  PATTERN (A B)
        |  DEFINE A AS price > 100
        |)
        |""".stripMargin,
      UnresolvedMatchRecognize(
        partitionSpec = Seq($"symbol"),
        orderSpec = Seq($"ts".asc),
        pattern = PatternSequence(Seq(PatternVariable("a"), PatternVariable("b"))),
        patternVariableDefinitions = Seq(($"price" > 100).as("a")),
        measures = Seq.empty,
        allRowsPerMatch = false,
        virtualColumns = MatchRecognizeVirtualColumns.empty,
        table("t")
      ).select(star())
    )
  }

  test("match_recognize - with measures") {
    assertEqual(
      """
        |SELECT * FROM t
        |MATCH_RECOGNIZE (
        |  MEASURES ts AS match_ts, price AS match_price
        |  PATTERN (A B)
        |  DEFINE A AS price > 100
        |)
        |""".stripMargin,
      UnresolvedMatchRecognize(
        partitionSpec = Seq.empty,
        orderSpec = Seq.empty,
        pattern = PatternSequence(Seq(PatternVariable("a"), PatternVariable("b"))),
        patternVariableDefinitions = Seq(($"price" > 100).as("a")),
        measures = Seq($"ts".as("match_ts"), $"price".as("match_price")),
        allRowsPerMatch = false,
        virtualColumns = MatchRecognizeVirtualColumns.empty,
        table("t")
      ).select(star())
    )
  }

  test("match_recognize - with all clauses") {
    assertEqual(
      """
        |SELECT * FROM t
        |MATCH_RECOGNIZE (
        |  PARTITION BY symbol
        |  ORDER BY ts
        |  MEASURES ts AS match_ts
        |  ONE ROW PER MATCH
        |  AFTER MATCH SKIP PAST LAST ROW
        |  PATTERN (A B C)
        |  DEFINE
        |    A AS price > 100,
        |    B AS price > 200,
        |    C AS price > 300
        |)
        |""".stripMargin,
      UnresolvedMatchRecognize(
        partitionSpec = Seq($"symbol"),
        orderSpec = Seq($"ts".asc),
        pattern = PatternSequence(Seq(
          PatternVariable("a"),
          PatternVariable("b"),
          PatternVariable("c")
        )),
        patternVariableDefinitions = Seq(
          ($"price" > 100).as("a"),
          ($"price" > 200).as("b"),
          ($"price" > 300).as("c")
        ),
        measures = Seq($"ts".as("match_ts")),
        allRowsPerMatch = false,
        virtualColumns = MatchRecognizeVirtualColumns.empty,
        table("t")
      ).select(star())
    )
  }

  test("match_recognize - pattern with quantifiers") {
    assertEqual(
      """
        |SELECT * FROM t
        |MATCH_RECOGNIZE (
        |  PATTERN (A+ B* C?)
        |  DEFINE A AS price > 100
        |)
        |""".stripMargin,
      UnresolvedMatchRecognize(
        partitionSpec = Seq.empty,
        orderSpec = Seq.empty,
        pattern = PatternSequence(Seq(
          QuantifiedPattern(PatternVariable("a"), OneOrMore),
          QuantifiedPattern(PatternVariable("b"), ZeroOrMore),
          QuantifiedPattern(PatternVariable("c"), ZeroOrOne)
        )),
        patternVariableDefinitions = Seq(($"price" > 100).as("a")),
        measures = Seq.empty,
        allRowsPerMatch = false,
        virtualColumns = MatchRecognizeVirtualColumns.empty,
        table("t")
      ).select(star())
    )
  }

  test("match_recognize - pattern with reluctant quantifiers") {
    assertEqual(
      """
        |SELECT * FROM t
        |MATCH_RECOGNIZE (
        |  PATTERN (A+? B*? C??)
        |  DEFINE A AS price > 100
        |)
        |""".stripMargin,
      UnresolvedMatchRecognize(
        partitionSpec = Seq.empty,
        orderSpec = Seq.empty,
        pattern = PatternSequence(Seq(
          QuantifiedPattern(PatternVariable("a"), OneOrMore, greedy = false),
          QuantifiedPattern(PatternVariable("b"), ZeroOrMore, greedy = false),
          QuantifiedPattern(PatternVariable("c"), ZeroOrOne, greedy = false)
        )),
        patternVariableDefinitions = Seq(($"price" > 100).as("a")),
        measures = Seq.empty,
        allRowsPerMatch = false,
        virtualColumns = MatchRecognizeVirtualColumns.empty,
        table("t")
      ).select(star())
    )
  }

  test("match_recognize - pattern with bounded quantifiers") {
    assertEqual(
      """
        |SELECT * FROM t
        |MATCH_RECOGNIZE (
        |  PATTERN (A{3} B{2,5} C{1,} D{,4})
        |  DEFINE A AS price > 100
        |)
        |""".stripMargin,
      UnresolvedMatchRecognize(
        partitionSpec = Seq.empty,
        orderSpec = Seq.empty,
        pattern = PatternSequence(Seq(
          QuantifiedPattern(PatternVariable("a"), ExactQuantifier(3)),
          QuantifiedPattern(PatternVariable("b"), RangeQuantifier(2, 5)),
          QuantifiedPattern(PatternVariable("c"), MinQuantifier(1)),
          QuantifiedPattern(PatternVariable("d"), RangeQuantifier(0, 4))
        )),
        patternVariableDefinitions = Seq(($"price" > 100).as("a")),
        measures = Seq.empty,
        allRowsPerMatch = false,
        virtualColumns = MatchRecognizeVirtualColumns.empty,
        table("t")
      ).select(star())
    )
  }

  test("match_recognize - pattern with reluctant bounded quantifiers") {
    assertEqual(
      """
        |SELECT * FROM t
        |MATCH_RECOGNIZE (
        |  PATTERN (A{2,4}? B{1,}? C{,3}?)
        |  DEFINE A AS price > 100
        |)
        |""".stripMargin,
      UnresolvedMatchRecognize(
        partitionSpec = Seq.empty,
        orderSpec = Seq.empty,
        pattern = PatternSequence(Seq(
          QuantifiedPattern(PatternVariable("a"), RangeQuantifier(2, 4), greedy = false),
          QuantifiedPattern(PatternVariable("b"), MinQuantifier(1), greedy = false),
          QuantifiedPattern(PatternVariable("c"), RangeQuantifier(0, 3), greedy = false)
        )),
        patternVariableDefinitions = Seq(($"price" > 100).as("a")),
        measures = Seq.empty,
        allRowsPerMatch = false,
        virtualColumns = MatchRecognizeVirtualColumns.empty,
        table("t")
      ).select(star())
    )
  }

  test("match_recognize - grouped pattern") {
    assertEqual(
      """
        |SELECT * FROM t
        |MATCH_RECOGNIZE (
        |  PATTERN ((A B)+)
        |  DEFINE A AS price > 100
        |)
        |""".stripMargin,
      UnresolvedMatchRecognize(
        partitionSpec = Seq.empty,
        orderSpec = Seq.empty,
        pattern = QuantifiedPattern(
          PatternSequence(Seq(PatternVariable("a"), PatternVariable("b"))),
          OneOrMore
        ),
        patternVariableDefinitions = Seq(($"price" > 100).as("a")),
        measures = Seq.empty,
        allRowsPerMatch = false,
        virtualColumns = MatchRecognizeVirtualColumns.empty,
        table("t")
      ).select(star())
    )
  }

  test("match_recognize - alternation pattern") {
    assertEqual(
      """
        |SELECT * FROM t
        |MATCH_RECOGNIZE (
        |  PATTERN (A | B)
        |  DEFINE A AS price > 100
        |)
        |""".stripMargin,
      UnresolvedMatchRecognize(
        partitionSpec = Seq.empty,
        orderSpec = Seq.empty,
        pattern = PatternAlternation(Seq(PatternVariable("a"), PatternVariable("b"))),
        patternVariableDefinitions = Seq(($"price" > 100).as("a")),
        measures = Seq.empty,
        allRowsPerMatch = false,
        virtualColumns = MatchRecognizeVirtualColumns.empty,
        table("t")
      ).select(star())
    )
  }

  test("match_recognize - permute pattern") {
    assertEqual(
      """
        |SELECT * FROM t
        |MATCH_RECOGNIZE (
        |  PATTERN (PERMUTE(A, B, C))
        |  DEFINE A AS price > 100
        |)
        |""".stripMargin,
      UnresolvedMatchRecognize(
        partitionSpec = Seq.empty,
        orderSpec = Seq.empty,
        pattern = PatternPermute(Seq(
          PatternVariable("a"), PatternVariable("b"), PatternVariable("c"))),
        patternVariableDefinitions = Seq(($"price" > 100).as("a")),
        measures = Seq.empty,
        allRowsPerMatch = false,
        virtualColumns = MatchRecognizeVirtualColumns.empty,
        table("t")
      ).select(star())
    )
  }

  test("match_recognize - permute in sequence") {
    assertEqual(
      """
        |SELECT * FROM t
        |MATCH_RECOGNIZE (
        |  PATTERN (X PERMUTE(A, B) Y)
        |  DEFINE A AS price > 100
        |)
        |""".stripMargin,
      UnresolvedMatchRecognize(
        partitionSpec = Seq.empty,
        orderSpec = Seq.empty,
        pattern = PatternSequence(Seq(
          PatternVariable("x"),
          PatternPermute(Seq(PatternVariable("a"), PatternVariable("b"))),
          PatternVariable("y")
        )),
        patternVariableDefinitions = Seq(($"price" > 100).as("a")),
        measures = Seq.empty,
        allRowsPerMatch = false,
        virtualColumns = MatchRecognizeVirtualColumns.empty,
        table("t")
      ).select(star())
    )
  }

  test("match_recognize - error on PERMUTE with single pattern") {
    intercept(
      """
        |SELECT * FROM t
        |MATCH_RECOGNIZE (
        |  PATTERN (PERMUTE(A))
        |  DEFINE A AS price > 100
        |)
        |""".stripMargin,
      Some("MATCH_RECOGNIZE_PERMUTE_TOO_FEW_PATTERNS")
    )
  }

  test("match_recognize - single variable pattern") {
    assertEqual(
      """
        |SELECT * FROM t
        |MATCH_RECOGNIZE (
        |  PATTERN (A)
        |  DEFINE A AS price > 100
        |)
        |""".stripMargin,
      UnresolvedMatchRecognize(
        partitionSpec = Seq.empty,
        orderSpec = Seq.empty,
        pattern = PatternVariable("a"),
        patternVariableDefinitions = Seq(($"price" > 100).as("a")),
        measures = Seq.empty,
        allRowsPerMatch = false,
        virtualColumns = MatchRecognizeVirtualColumns.empty,
        table("t")
      ).select(star())
    )
  }

  test("match_recognize - with alias") {
    Seq(
      "SELECT mr.* FROM t MATCH_RECOGNIZE (PATTERN (A) DEFINE A AS x > 1) mr",
      "SELECT mr.* FROM t MATCH_RECOGNIZE (PATTERN (A) DEFINE A AS x > 1) AS mr"
    ).foreach { sql =>
      withClue(sql) {
        assertEqual(
          sql,
          UnresolvedMatchRecognize(
            partitionSpec = Seq.empty,
            orderSpec = Seq.empty,
            pattern = PatternVariable("a"),
            patternVariableDefinitions = Seq(($"x" > 1).as("a")),
            measures = Seq.empty,
            allRowsPerMatch = false,
            virtualColumns = MatchRecognizeVirtualColumns.empty,
            table("t")
          ).subquery("mr").select(star("mr"))
        )
      }
    }
  }

  test("match_recognize - error on unaliased measure") {
    // With aliasedExpressionSeq, unaliased measures are now caught at parse time
    intercept(
      """
        |SELECT * FROM t
        |MATCH_RECOGNIZE (
        |  MEASURES ts
        |  PATTERN (A)
        |  DEFINE A AS price > 100
        |)
        |""".stripMargin,
      Some("PARSE_SYNTAX_ERROR")
    )
  }

  test("match_recognize - with join") {
    assertEqual(
      """
        |SELECT * FROM t1
        |MATCH_RECOGNIZE (
        |  PATTERN (A)
        |  DEFINE A AS x > 1
        |)
        |JOIN t2
        |""".stripMargin,
      UnresolvedMatchRecognize(
        partitionSpec = Seq.empty,
        orderSpec = Seq.empty,
        pattern = PatternVariable("a"),
        patternVariableDefinitions = Seq(($"x" > 1).as("a")),
        measures = Seq.empty,
        allRowsPerMatch = false,
        virtualColumns = MatchRecognizeVirtualColumns.empty,
        table("t1")
      ).join(table("t2")).select(star())
    )
  }

  test("match_recognize - ALL ROWS PER MATCH") {
    assertEqual(
      """
        |SELECT * FROM t
        |MATCH_RECOGNIZE (
        |  ALL ROWS PER MATCH
        |  PATTERN (A)
        |  DEFINE A AS price > 100
        |)
        |""".stripMargin,
      UnresolvedMatchRecognize(
        partitionSpec = Seq.empty,
        orderSpec = Seq.empty,
        pattern = PatternVariable("a"),
        patternVariableDefinitions = Seq(($"price" > 100).as("a")),
        measures = Seq.empty,
        allRowsPerMatch = true,
        virtualColumns = MatchRecognizeVirtualColumns.empty,
        table("t")
      ).select(star())
    )
  }

  test("match_recognize - ALL ROWS PER MATCH SHOW EMPTY MATCHES") {
    assertEqual(
      """
        |SELECT * FROM t
        |MATCH_RECOGNIZE (
        |  ALL ROWS PER MATCH SHOW EMPTY MATCHES
        |  PATTERN (A)
        |  DEFINE A AS price > 100
        |)
        |""".stripMargin,
      UnresolvedMatchRecognize(
        partitionSpec = Seq.empty,
        orderSpec = Seq.empty,
        pattern = PatternVariable("a"),
        patternVariableDefinitions = Seq(($"price" > 100).as("a")),
        measures = Seq.empty,
        allRowsPerMatch = true,
        virtualColumns = MatchRecognizeVirtualColumns.empty,
        table("t")
      ).select(star())
    )
  }

  test("match_recognize - error on OMIT EMPTY MATCHES") {
    intercept(
      """
        |SELECT * FROM t
        |MATCH_RECOGNIZE (
        |  ALL ROWS PER MATCH OMIT EMPTY MATCHES
        |  PATTERN (A)
        |  DEFINE A AS price > 100
        |)
        |""".stripMargin,
      Some("UNSUPPORTED_FEATURE.MATCH_RECOGNIZE_ROWS_PER_MATCH_MODE")
    )
  }

  test("match_recognize - error on WITH UNMATCHED ROWS") {
    intercept(
      """
        |SELECT * FROM t
        |MATCH_RECOGNIZE (
        |  ALL ROWS PER MATCH WITH UNMATCHED ROWS
        |  PATTERN (A)
        |  DEFINE A AS price > 100
        |)
        |""".stripMargin,
      Some("UNSUPPORTED_FEATURE.MATCH_RECOGNIZE_ROWS_PER_MATCH_MODE")
    )
  }

  test("match_recognize - AFTER MATCH SKIP PAST LAST ROW") {
    // SKIP PAST LAST ROW is the default and supported mode - same plan as without the clause
    assertEqual(
      """
        |SELECT * FROM t
        |MATCH_RECOGNIZE (
        |  AFTER MATCH SKIP PAST LAST ROW
        |  PATTERN (A B)
        |  DEFINE A AS price > 100
        |)
        |""".stripMargin,
      UnresolvedMatchRecognize(
        partitionSpec = Seq.empty,
        orderSpec = Seq.empty,
        pattern = PatternSequence(Seq(PatternVariable("a"), PatternVariable("b"))),
        patternVariableDefinitions = Seq(($"price" > 100).as("a")),
        measures = Seq.empty,
        allRowsPerMatch = false,
        virtualColumns = MatchRecognizeVirtualColumns.empty,
        table("t")
      ).select(star())
    )
  }

  test("match_recognize - error on AFTER MATCH SKIP TO NEXT ROW") {
    intercept(
      """
        |SELECT * FROM t
        |MATCH_RECOGNIZE (
        |  AFTER MATCH SKIP TO NEXT ROW
        |  PATTERN (A)
        |  DEFINE A AS price > 100
        |)
        |""".stripMargin,
      Some("UNSUPPORTED_FEATURE.MATCH_RECOGNIZE_AFTER_MATCH_SKIP_MODE")
    )
  }

  test("match_recognize - error on AFTER MATCH SKIP TO FIRST") {
    intercept(
      """
        |SELECT * FROM t
        |MATCH_RECOGNIZE (
        |  AFTER MATCH SKIP TO FIRST A
        |  PATTERN (A)
        |  DEFINE A AS price > 100
        |)
        |""".stripMargin,
      Some("UNSUPPORTED_FEATURE.MATCH_RECOGNIZE_AFTER_MATCH_SKIP_MODE")
    )
  }

  test("match_recognize - error on AFTER MATCH SKIP TO LAST") {
    intercept(
      """
        |SELECT * FROM t
        |MATCH_RECOGNIZE (
        |  AFTER MATCH SKIP TO LAST A
        |  PATTERN (A)
        |  DEFINE A AS price > 100
        |)
        |""".stripMargin,
      Some("UNSUPPORTED_FEATURE.MATCH_RECOGNIZE_AFTER_MATCH_SKIP_MODE")
    )
  }

  test("match_recognize - error on non-named partition expression") {
    // PARTITION BY expression must be a column or have an alias
    intercept(
      """
        |SELECT * FROM t
        |MATCH_RECOGNIZE (
        |  PARTITION BY a + b
        |  PATTERN (A)
        |  DEFINE A AS price > 100
        |)
        |""".stripMargin,
      Some("MATCH_RECOGNIZE_PARTITION_BY_MUST_BE_NAMED"),
      "a+b"
    )
  }
}
