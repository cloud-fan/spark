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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{Expression, Ascending, SortOrder}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._

class SortOptimizeSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Operator Optimizations", FixedPoint(100),
        ColumnPruning,
        ProjectCollapsing,
        RemoveUnnecessarySortOrderEvaluation) :: Nil
  }

  private val testRelation = LocalRelation('a.int, 'b.int)

  private def order(e: Expression) = SortOrder(e, Ascending)

  test("sort on projection") {
    val expr: Expression = ('a + 2) * 3

    val query =
      testRelation
        .select(expr.as("eval"), 'b)
        .orderBy(order(expr)).analyze
    val optimized = Optimize.execute(query)

    val correctAnswer =
      testRelation
        .select(expr.as("eval"), 'b)
        .orderBy(order('eval)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("sort on projection: partial replaced") {
    val projectExpr: Expression = 'a + 2
    val orderExpr: Expression = projectExpr * 3

    val query =
      testRelation
        .select(projectExpr.as("eval"), 'b)
        .orderBy(order(orderExpr)).analyze
    val optimized = Optimize.execute(query)

    val correctAnswer =
      testRelation
        .select(projectExpr.as("eval"), 'b)
        .orderBy(order('eval * 3)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("sort on aggregation") {
    val expr: Expression = ('a + 2) * 3

    val query =
      testRelation
        .groupBy('a)(expr.as("eval"), sum('b))
        .orderBy(order(expr)).analyze

    val optimized = Optimize.execute(query)

    val correctAnswer =
      testRelation
        .groupBy('a)(expr.as("eval"), sum('b))
        .orderBy(order('eval)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("sort on aggregation: partial replaced") {
    val projectExpr: Expression = 'a + 2
    val orderExpr: Expression = projectExpr * 3

    val query =
      testRelation
        .groupBy('a)(projectExpr.as("eval"), sum('b))
        .orderBy(order(orderExpr)).analyze
    val optimized = Optimize.execute(query)

    val correctAnswer =
      testRelation
        .groupBy('a)(projectExpr.as("eval"), sum('b))
        .orderBy(order('eval * 3)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("sort on aggregation: group by aggregate expression") {
    val query =
      testRelation
        .groupBy('a)('a)
        .orderBy(order(sum('b)), order(sum('b) + 1)).analyze

    val optimized = Optimize.execute(query)

    val correctAnswer =
      testRelation
        .groupBy('a)('a, sum('b).as("aggOrder"))
        .orderBy(order('aggOrder), order('aggOrder + 1))
        .select('a).analyze

    comparePlans(optimized, correctAnswer)
  }
}
