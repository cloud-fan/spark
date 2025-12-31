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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Last}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_MATCH_RECOGNIZE
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.errors.QueryCompilationErrors.toSQLId
import org.apache.spark.sql.types.{LongType, StringType}

/**
 * Resolves UnresolvedMatchRecognize to MatchRecognize + MatchRecognizeMeasures.
 *
 * This rule triggers only when UnresolvedMatchRecognize has its child and all
 * expressions resolved. It:
 * 1. Creates a resolved MatchRecognize with matchNumberAttr and classifierAttr
 * 2. Rewrites measure expressions to use original child.output attributes with classifier filters
 * 3. Creates a MatchRecognizeMeasures node on top to aggregate the results
 *
 * For aggregate expressions with PATTERN variable qualified attributes (e.g., LAST(A.price)):
 * - Rewrite to use original child.output attribute (e.g., price)
 * - Add a classifier filter (WHERE _classifier = 'A')
 *
 * For PATTERN variable qualified column references appearing alone (not in aggregate):
 * - Wrap with LAST function and add classifier filter
 */
object ResolveMatchRecognize extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsUpWithPruning(_.containsPattern(UNRESOLVED_MATCH_RECOGNIZE)) {
      case m: UnresolvedMatchRecognize if canResolve(m) =>
        resolveMatchRecognize(m)
    }
  }

  /**
   * Check if UnresolvedMatchRecognize is ready to be resolved.
   * We check specific conditions instead of using `resolved` because
   * UnresolvedMatchRecognize always returns resolved=false.
   */
  private def canResolve(m: UnresolvedMatchRecognize): Boolean = {
    m.matchedRowsAttrs.nonEmpty &&
      m.child.resolved &&
      m.partitionSpec.forall(_.resolved) &&
      m.orderSpec.forall(_.resolved) &&
      m.patternVariableDefinitions.forall(_.resolved) &&
      m.measures.forall(_.resolved)
  }

  /**
   * Validate the MATCH_RECOGNIZE plan before transforming it.
   * Throws AnalysisException for invalid patterns, duplicate or undefined variables,
   * and measure expression validity.
   */
  private def validateMatchRecognize(m: UnresolvedMatchRecognize): Unit = {
    // Check for duplicate pattern variable names
    val varNames = m.patternVariableDefinitions.map(_.name)
    val duplicates = varNames.diff(varNames.distinct).distinct
    if (duplicates.nonEmpty) {
      m.failAnalysis(
        errorClass = "DUPLICATE_PATTERN_VARIABLE",
        messageParameters = Map("variableName" -> toSQLId(duplicates.head)))
    }

    // Check that all pattern variables are defined
    if (m.undefinedVariables.nonEmpty) {
      m.failAnalysis(
        errorClass = "UNDEFINED_PATTERN_VARIABLE",
        messageParameters = Map("variableName" -> toSQLId(m.undefinedVariables.head)))
    }

    // Check that DEFINE clause predicates are deterministic.
    // Nondeterministic functions (rand, uuid, etc.) are prohibited because NFA simulation
    // evaluates predicates multiple times per row from different active states, which
    // would produce inconsistent results.
    m.patternVariableDefinitions.foreach { alias =>
      alias.child.foreach {
        case e if !e.deterministic =>
          m.failAnalysis(
            errorClass = "MATCH_RECOGNIZE_NONDETERMINISTIC_DEFINE",
            messageParameters = Map(
              "expression" -> toPrettySQL(e),
              "variableName" -> toSQLId(alias.name)))
        case _ =>
      }
    }

    // Check pattern validity: alternation is not yet supported
    def checkPattern(pattern: RowPattern): Unit = pattern match {
      case _: PatternVariable => // OK
      case PatternSequence(patterns) => patterns.foreach(checkPattern)
      case QuantifiedPattern(inner, _, _) =>
        checkPattern(inner)
      case _: PatternAlternation =>
        m.failAnalysis(
          errorClass = "UNSUPPORTED_FEATURE.MATCH_RECOGNIZE_ALTERNATION",
          messageParameters = Map.empty)
    }
    checkPattern(m.pattern)

    // Check that column references in measures are valid.
    // Like Aggregate, expressions must be either:
    // - Semantically equal to a partition output attribute
    // - Wrapped by an aggregate function
    // - Qualified with a PATTERN variable (will be auto-wrapped with LAST)
    // matchedRowsAttrs layout: [partition attrs] + [unqualified child attrs] +
    //   [PATTERN variable qualified attrs]
    val numPartitionAttrs = m.partitionSpec.size
    val numChildAttrs = m.child.output.size
    val partitionAttrs = m.matchedRowsAttrs.take(numPartitionAttrs)
    val patternVarQualifiedAttrs = m.matchedRowsAttrs.drop(numPartitionAttrs + numChildAttrs)
    // Attributes allowed without being wrapped in aggregate: partition attrs and
    // PATTERN variable qualified attrs (which will be auto-wrapped with LAST)
    val allowedWithoutAggAttrs = AttributeSet(partitionAttrs ++ patternVarQualifiedAttrs)
    def checkValidMeasureExpression(expr: Expression): Unit = expr match {
      case ae: AggregateExpression =>
        // Validate nested aggregates and non-deterministic expressions
        ExprUtils.checkValidAggregateExpression(ae)
      case a: Attribute if allowedWithoutAggAttrs.contains(a) =>
        // Partition attrs and PATTERN variable qualified attrs are OK
      case e: Attribute =>
        // Other attribute references must be wrapped by aggregates
        m.failAnalysis(
          errorClass = "MATCH_RECOGNIZE_INVALID_MEASURE_EXPRESSION",
          messageParameters = Map("expression" -> toPrettySQL(e)))
      case e => e.children.foreach(checkValidMeasureExpression)
    }
    m.measures.foreach(alias => checkValidMeasureExpression(alias.child))
  }

  private def resolveMatchRecognize(m: UnresolvedMatchRecognize): LogicalPlan = {
    // Validate the plan before transformation
    validateMatchRecognize(m)

    // Create internal attributes for match tracking
    val matchNumberAttr = AttributeReference("_match_number", LongType, nullable = false)()
    val classifierAttr = AttributeReference("_classifier", StringType, nullable = false)()

    // Create resolved MatchRecognize
    val matchRecognize = MatchRecognize(
      m.partitionSpec,
      m.orderSpec,
      m.pattern,
      m.patternVariableDefinitions,
      matchNumberAttr,
      classifierAttr,
      m.child
    )

    val numPartitionAttrs = m.partitionSpec.size
    val numChildAttrs = m.child.output.size
    // matchedRowsAttrs layout: [partition attrs] + [unqualified child attrs] +
    // one or more groups of [PATTERN variable qualified attrs] (one group per PATTERN variable).
    // These all have fresh ExprIds for resolution. After rewriting we use original child.output.
    val partitionAttrs = m.matchedRowsAttrs.take(numPartitionAttrs)
    val partitionOutputAttrs = m.partitionSpec.map(_.toAttribute)
    // Build mapping from fresh ExprId attrs (used during resolution) to original child.output
    // numRowAttrGroups = 1 (unqualified) + N (one per PATTERN variable)
    val numRowAttrGroups = m.patternVariableDefinitions.length + 1
    val numRowAttrs = m.matchedRowsAttrs.length - numPartitionAttrs
    assert(
      numRowAttrGroups * numChildAttrs == numRowAttrs,
      s"Expected matchedRowsAttrs to contain $numRowAttrGroups groups of $numChildAttrs attrs, " +
        s"got $numRowAttrs attrs")
    val matchedAttrToOutput = AttributeMap(
      partitionAttrs.zip(partitionOutputAttrs) ++
        m.matchedRowsAttrs.drop(numPartitionAttrs)
          .zip(Seq.fill(numRowAttrGroups)(m.child.output).flatten)
    )

    // Rewrite measure expressions
    val rewrittenMeasures = m.measures.map { alias =>
      val rewrittenChild = rewriteMeasureExpression(
        alias.child, matchedAttrToOutput, classifierAttr, m)
      alias.withNewChild(rewrittenChild)
    }

    // Grouping expressions: partition columns + match number
    val groupingExprs = partitionOutputAttrs :+ matchNumberAttr

    // Create MatchRecognizeMeasures on top
    MatchRecognizeMeasures(groupingExprs, rewrittenMeasures, matchRecognize)
  }

  /**
   * Rewrites a measure expression to use original child.output attributes with classifier filters.
   *
   * For aggregate expressions with PATTERN variable qualified attributes:
   * - Rewrite to use original child.output attribute
   * - Add a classifier filter (WHERE _classifier = 'variable_name')
   *
   * For PATTERN variable qualified column references appearing alone (not in aggregate):
   * - Wrap with LAST function and add classifier filter
   *
   * Uses transformDown to handle aggregates first, preventing qualified attrs inside
   * aggregate functions from being wrapped in LAST (which would cause nested agg issues).
   */
  private def rewriteMeasureExpression(
      expr: Expression,
      matchedAttrToOutput: AttributeMap[Attribute],
      classifierAttr: AttributeReference,
      m: UnresolvedMatchRecognize): Expression = {

    expr.transformDown {
      case ae: AggregateExpression =>
        rewriteAggregate(ae, matchedAttrToOutput, classifierAttr, m)

      case a: Attribute if matchedAttrToOutput.contains(a) =>
        val childAttr = matchedAttrToOutput(a)
        if (a.qualifier.nonEmpty) {
          // PATTERN variable qualified column reference not in aggregate - wrap with LAST
          val qualifier = a.qualifier.head
          val classifierFilter = EqualTo(classifierAttr, Literal(qualifier))
          AggregateExpression(
            Last(childAttr, ignoreNulls = false),
            Complete,
            isDistinct = false,
            filter = Some(classifierFilter)
          )
        } else {
          // Unqualified attr (including partition attrs) - just replace with child.output attr
          childAttr
        }
    }
  }

  /**
   * Rewrites an aggregate expression to use original child.output attributes and classifier filter.
   * Throws an error if the aggregate contains attributes with mixed PATTERN variable qualifiers.
   */
  private def rewriteAggregate(
      ae: AggregateExpression,
      matchedAttrToOutput: AttributeMap[Attribute],
      classifierAttr: AttributeReference,
      m: UnresolvedMatchRecognize): AggregateExpression = {
    // Check that all attributes in the aggregate have the same qualifier
    val attrs = ae.collect { case a: Attribute if matchedAttrToOutput.contains(a) => a }
    val qualifiers = attrs.map(_.qualifier).distinct
    if (qualifiers.size > 1) {
      m.failAnalysis(
        errorClass = "MATCH_RECOGNIZE_AGGREGATE_MIXED_QUALIFIERS",
        messageParameters = Map(
          "expression" -> toPrettySQL(ae),
          "qualifiers" -> qualifiers.map(_.mkString(".")).mkString(", ")))
    }

    // Extract PATTERN variable qualifier from matched attributes
    val qualifier = attrs.headOption.flatMap(_.qualifier.headOption)

    // Rewrite all attributes to original child.output versions
    val rewritten = ae.transform {
      case a: Attribute => matchedAttrToOutput.getOrElse(a, a)
    }.asInstanceOf[AggregateExpression].copy(mode = Complete)

    // Add classifier filter for PATTERN variable qualified aggregates
    qualifier.map { name =>
      val classifierFilter = EqualTo(classifierAttr, Literal(name))
      val combinedFilter =
        rewritten.filter.map(f => And(f, classifierFilter)).orElse(Some(classifierFilter))
      rewritten.copy(filter = combinedFilter)
    }.getOrElse(rewritten)
  }
}
