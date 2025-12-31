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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Complete}
import org.apache.spark.sql.catalyst.planning.PhysicalAggregation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, MatchRecognizeAndMeasure}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan, SparkStrategy => Strategy}
import org.apache.spark.sql.execution.aggregate.SortAggregateExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.types.{LongType, StringType}

/**
 * Strategy for planning MatchRecognizeAndMeasure.
 *
 * Decomposes the single logical plan into physical operators depending on the output mode:
 *
 * ONE ROW PER MATCH: MatchRecognizeExec + SortAggregateExec
 *   - SortAggregateExec groups by (partition_cols, _match_number) to produce one summary row
 *   - Measures use FINAL semantics (aggregate over entire match)
 *
 * ALL ROWS PER MATCH: MatchRecognizeExec + WindowExec + ProjectExec
 *   - WindowExec partitions by (partition_cols, _match_number) and computes running
 *     aggregates with frame ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
 *   - ProjectExec selects final output columns, dropping internal attributes
 *
 * Both modes rewrite measure expressions from their resolved form (referencing
 * virtualColumns with pattern variable qualifiers) to their executable form.
 */
object MatchRecognizeStrategy extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case m: logical.MatchRecognizeAndMeasure =>
      val matchNumberAttr = AttributeReference("_match_number", LongType, nullable = false)()
      val classifierAttr = AttributeReference("_classifier", StringType, nullable = false)()

      val matchExec = MatchRecognizeExec(
        m.partitionSpec, m.orderSpec, m.pattern, m.patternVariableDefinitions,
        matchNumberAttr, classifierAttr, planLater(m.child))

      if (m.allRowsPerMatch) {
        planAllRowsPerMatch(m, matchExec, matchNumberAttr, classifierAttr)
      } else {
        planOneRowPerMatch(m, matchExec, matchNumberAttr, classifierAttr)
      }

    case _ => Nil
  }

  private def planOneRowPerMatch(
      m: MatchRecognizeAndMeasure,
      matchExec: MatchRecognizeExec,
      matchNumberAttr: AttributeReference,
      classifierAttr: AttributeReference): List[SparkPlan] = {
    val rewrittenMeasures = rewriteMeasuresForAggregate(m, classifierAttr)

    val partitionOutputAttrs = m.partitionSpec.map(_.toAttribute)
    val groupingExprs: Seq[Attribute] = partitionOutputAttrs :+ matchNumberAttr

    val aggregateOutput: Seq[NamedExpression] = partitionOutputAttrs ++ rewrittenMeasures

    // Construct a temporary Aggregate to leverage PhysicalAggregation's extraction
    // of aggregate expressions and result expressions. m.child is used as a placeholder;
    // the actual physical child is matchExec (passed to SortAggregateExec below).
    val PhysicalAggregation(_, aggExprs, resultExprs, _) =
      Aggregate(groupingExprs, aggregateOutput, m.child)
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
      child = matchExec) :: Nil
  }

  private def planAllRowsPerMatch(
      m: MatchRecognizeAndMeasure,
      matchExec: MatchRecognizeExec,
      matchNumberAttr: AttributeReference,
      classifierAttr: AttributeReference): List[SparkPlan] = {
    val virtualColToOutput = buildAttrMapping(m)
    val partitionOutputAttrs = m.partitionSpec.map(_.toAttribute)
    val windowPartition: Seq[Expression] = partitionOutputAttrs :+ matchNumberAttr
    val windowFrame = SpecifiedWindowFrame(
      RowFrame, UnboundedPreceding, CurrentRow)
    val windowSpec = WindowSpecDefinition(windowPartition, m.orderSpec, windowFrame)

    val windowExprs: Seq[NamedExpression] = m.measures.map { alias =>
      val windowExpr = rewriteMeasureForWindow(
        alias.child, virtualColToOutput, classifierAttr, windowSpec)
      Alias(windowExpr, alias.name)(exprId = alias.exprId)
    }

    val windowExec = WindowExec(windowExprs, windowPartition, m.orderSpec, matchExec)

    // Project to select only output columns: child.output + measures
    val outputExprs: Seq[NamedExpression] =
      m.child.output ++ windowExprs.map(_.toAttribute)
    ProjectExec(outputExprs, windowExec) :: Nil
  }

  /**
   * Builds a mapping from virtualColumns ExprIds to the original child.output attributes.
   */
  private def buildAttrMapping(m: MatchRecognizeAndMeasure): AttributeMap[Attribute] = {
    val partitionOutputAttrs = m.partitionSpec.map(_.toAttribute)
    val numRowAttrGroups = m.patternVariableDefinitions.length + 1
    AttributeMap(
      m.virtualColumns.partitionAttrs.zip(partitionOutputAttrs) ++
        (m.virtualColumns.unqualifiedAttrs ++ m.virtualColumns.allQualifiedAttrs)
          .zip(Seq.fill(numRowAttrGroups)(m.child.output).flatten)
    )
  }

  // ---- ONE ROW PER MATCH: aggregate-based measure rewriting ----

  /**
   * Rewrites measures for ONE ROW PER MATCH using aggregate expressions with
   * classifier filters (FINAL semantics - aggregate over the entire match).
   */
  private def rewriteMeasuresForAggregate(
      m: MatchRecognizeAndMeasure,
      classifierAttr: AttributeReference): Seq[Alias] = {
    val virtualColToOutput = buildAttrMapping(m)
    m.measures.map { alias =>
      val rewrittenChild = rewriteMeasureForAggregate(
        alias.child, virtualColToOutput, classifierAttr)
      alias.withNewChild(rewrittenChild)
    }
  }

  private def rewriteMeasureForAggregate(
      expr: Expression,
      virtualColToOutput: AttributeMap[Attribute],
      classifierAttr: AttributeReference): Expression = {
    // Bare column references have already been wrapped with LAST by the analyzer.
    // This method only needs to handle AggregateExpressions and map attribute references.
    expr match {
      case ae: AggregateExpression =>
        rewriteAggregateExpr(ae, virtualColToOutput, classifierAttr)

      case a: Attribute if virtualColToOutput.contains(a) =>
        // Partition attributes: they are grouping columns, pass through directly.
        virtualColToOutput(a)

      case other =>
        other.mapChildren(child =>
          rewriteMeasureForAggregate(child, virtualColToOutput, classifierAttr))
    }
  }

  private def rewriteAggregateExpr(
      ae: AggregateExpression,
      virtualColToOutput: AttributeMap[Attribute],
      classifierAttr: AttributeReference): AggregateExpression = {
    val attrs = ae.collect { case a: Attribute if virtualColToOutput.contains(a) => a }
    // Uses the first attribute's qualifier. This is safe because the analysis phase
    // (MATCH_RECOGNIZE_AGGREGATE_MIXED_QUALIFIERS) rejects aggregates that mix
    // different pattern variable qualifiers.
    val qualifier = attrs.headOption.flatMap(_.qualifier.headOption)

    val rewritten = ae.transform {
      case a: Attribute => virtualColToOutput.getOrElse(a, a)
    }.asInstanceOf[AggregateExpression].copy(mode = Complete)

    qualifier.map { name =>
      val classifierFilter = EqualTo(classifierAttr, Literal(name))
      val combinedFilter =
        rewritten.filter.map(f => And(f, classifierFilter)).orElse(Some(classifierFilter))
      rewritten.copy(filter = combinedFilter)
    }.getOrElse(rewritten)
  }

  // ---- ALL ROWS PER MATCH: window-based measure rewriting ----

  /**
   * Rewrites a single measure expression for ALL ROWS PER MATCH using window functions
   * with RUNNING semantics (frame = ROWS UNBOUNDED PRECEDING TO CURRENT ROW).
   *
   * Pattern-variable-qualified aggregates use AggregateExpression.filter with
   * _classifier='X' to select rows belonging to that pattern variable. The filter
   * operates at the row level (rows not matching the filter don't update the
   * aggregate buffer at all), so each aggregate function's own NULL handling
   * semantics are preserved correctly.
   */
  private def rewriteMeasureForWindow(
      expr: Expression,
      virtualColToOutput: AttributeMap[Attribute],
      classifierAttr: AttributeReference,
      windowSpec: WindowSpecDefinition): Expression = {
    // Cannot use transformDown because the returned WindowExpression contains
    // windowSpec children that should not be traversed for further rewriting.
    expr match {
      case ae: AggregateExpression =>
        rewriteAggregateAsWindow(ae, virtualColToOutput, classifierAttr, windowSpec)

      case a: Attribute if virtualColToOutput.contains(a) =>
        virtualColToOutput(a)

      case other =>
        other.mapChildren(child =>
          rewriteMeasureForWindow(child, virtualColToOutput, classifierAttr, windowSpec))
    }
  }

  /**
   * Converts an AggregateExpression into a WindowExpression for running semantics.
   * For qualified aggregates, adds a classifier filter to the AggregateExpression.
   */
  private def rewriteAggregateAsWindow(
      ae: AggregateExpression,
      virtualColToOutput: AttributeMap[Attribute],
      classifierAttr: AttributeReference,
      windowSpec: WindowSpecDefinition): Expression = {
    val attrs = ae.collect { case a: Attribute if virtualColToOutput.contains(a) => a }
    val qualifier = attrs.headOption.flatMap(_.qualifier.headOption)

    val mappedFunc = ae.aggregateFunction.transform {
      case a: Attribute => virtualColToOutput.getOrElse(a, a)
    }.asInstanceOf[AggregateFunction]

    val classifierFilter = qualifier.map(name => EqualTo(classifierAttr, Literal(name)))
    val combinedFilter =
      (ae.filter, classifierFilter) match {
        case (Some(f), Some(cf)) => Some(And(f, cf))
        case (f @ Some(_), None) => f
        case (None, cf @ Some(_)) => cf
        case (None, None) => None
      }
    val windowAggExpr = AggregateExpression(
      mappedFunc, Complete, isDistinct = false, filter = combinedFilter)
    WindowExpression(windowAggExpr, windowSpec)
  }
}
