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
        matchedRowsAttrs = Nil,
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
        matchedRowsAttrs = Nil,
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
        matchedRowsAttrs = Nil,
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
        matchedRowsAttrs = Nil,
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
        matchedRowsAttrs = Nil,
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
        matchedRowsAttrs = Nil,
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
        matchedRowsAttrs = Nil,
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
        matchedRowsAttrs = Nil,
        table("t")
      ).select(star())
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
        matchedRowsAttrs = Nil,
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
            matchedRowsAttrs = Nil,
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
        matchedRowsAttrs = Nil,
        table("t1")
      ).join(table("t2")).select(star())
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
