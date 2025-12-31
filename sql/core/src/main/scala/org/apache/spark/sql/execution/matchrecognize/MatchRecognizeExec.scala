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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.RowPattern
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * Physical execution operator for MATCH_RECOGNIZE pattern matching.
 *
 * This operator processes sorted partitions to find pattern matches and produces
 * matched rows with internal classifier/match number columns. The planner aggregates
 * these rows to evaluate the MEASURES clause.
 *
 * It follows the WindowExec pattern for handling PARTITION BY and ORDER BY requirements.
 *
 * @param partitionSpec Named expressions for partitioning the input data
 * @param orderSpec Sort order within each partition
 * @param pattern The pattern expression AST (e.g., PatternSequence for "A B C")
 * @param patternVariableDefinitions Definitions for pattern variables (DEFINE clause)
 * @param matchNumberAttr Internal attribute for match number (unique within partition)
 * @param classifierAttr Internal attribute for pattern variable name per matched row
 * @param child The child physical plan
 */
case class MatchRecognizeExec(
    partitionSpec: Seq[NamedExpression],
    orderSpec: Seq[SortOrder],
    pattern: RowPattern,
    patternVariableDefinitions: Seq[Alias],
    matchNumberAttr: AttributeReference,
    classifierAttr: AttributeReference,
    child: SparkPlan)
  extends UnaryExecNode {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numMatches" -> SQLMetrics.createMetric(sparkContext, "number of matches found")
  )

  private val partitionOutputAttrs = partitionSpec.map(_.toAttribute)

  // Output includes partition columns, internal columns, then matched row columns
  override def output: Seq[Attribute] =
    partitionOutputAttrs ++ Seq(matchNumberAttr, classifierAttr) ++ child.output

  // These attributes are produced by this operator, not required from child
  override def producedAttributes: AttributeSet =
    AttributeSet(Seq(matchNumberAttr, classifierAttr))

  // Get underlying expressions from NamedExpression for distribution/ordering
  private val partitionExprs: Seq[Expression] = partitionSpec.map {
    case a: Alias => a.child
    case other => other
  }

  // Similar to WindowExecBase: require data to be clustered by partition columns
  override def requiredChildDistribution: Seq[Distribution] = {
    if (partitionSpec.isEmpty) {
      logWarning("No Partition Defined for MATCH_RECOGNIZE operation! Moving all data to a single "
        + "partition, this can cause serious performance degradation.")
      AllTuples :: Nil
    } else {
      ClusteredDistribution(partitionExprs) :: Nil
    }
  }

  // Data must be sorted by partition columns + order columns before processing
  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(partitionExprs.map(SortOrder(_, Ascending)) ++ orderSpec)

  override def outputOrdering: Seq[SortOrder] =
    partitionOutputAttrs.map(SortOrder(_, Ascending)) ++
      Seq(SortOrder(matchNumberAttr, Ascending))

  override def outputPartitioning: Partitioning = child.outputPartitioning

  protected override def doExecute(): RDD[InternalRow] = {
    val evaluatorFactory = new MatchRecognizeEvaluatorFactory(
      partitionExprs,
      partitionOutputAttrs,
      pattern,
      patternVariableDefinitions,
      matchNumberAttr,
      classifierAttr,
      child.output,
      longMetric("numMatches")
    )

    if (conf.usePartitionEvaluator) {
      child.execute().mapPartitionsWithEvaluator(evaluatorFactory)
    } else {
      child.execute().mapPartitionsWithIndex { (index, rowIterator) =>
        val evaluator = evaluatorFactory.createEvaluator()
        evaluator.eval(index, rowIterator)
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): MatchRecognizeExec =
    copy(child = newChild)
}
