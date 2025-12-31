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

import org.apache.spark.sql.catalyst.planning.PhysicalAggregation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy => Strategy}
import org.apache.spark.sql.execution.aggregate.SortAggregateExec

/**
 * Strategy for planning MATCH_RECOGNIZE operators.
 *
 * Plans two logical operators:
 * - MatchRecognize -> MatchRecognizeExec
 * - MatchRecognizeMeasures -> SortAggregateExec (uses sort aggregate because input is
 *   already sorted by partition columns + match_number)
 */
object MatchRecognizeStrategy extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case logical.MatchRecognize(partitionSpec, orderSpec, pattern,
        patternVariableDefinitions, matchNumberAttr, classifierAttr, child) =>
      MatchRecognizeExec(partitionSpec, orderSpec, pattern, patternVariableDefinitions,
        matchNumberAttr, classifierAttr, planLater(child)) :: Nil

    case logical.MatchRecognizeMeasures(groupingExprs, measures, child) =>
      // Use PhysicalAggregation to extract aggregate expressions
      val PhysicalAggregation(_, aggExprs, resultExprs, _) =
        Aggregate(groupingExprs, measures, child)
      val aggregateAttributes = aggExprs.map(_.resultAttribute)
      SortAggregateExec(
        requiredChildDistributionExpressions = None,
        isStreaming = false,
        numShufflePartitions = None,
        groupingExpressions = groupingExprs,
        aggregateExpressions = aggExprs,
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = groupingExprs.length,
        resultExpressions = resultExprs,
        child = planLater(child)) :: Nil

    case _ => Nil
  }
}
