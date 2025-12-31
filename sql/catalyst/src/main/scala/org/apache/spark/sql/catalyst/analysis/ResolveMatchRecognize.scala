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
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Last}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_MATCH_RECOGNIZE
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.errors.QueryCompilationErrors.toSQLId

/**
 * Resolves UnresolvedMatchRecognize to MatchRecognizeAndMeasure.
 *
 * This rule triggers only when UnresolvedMatchRecognize has its child and all
 * expressions resolved. It:
 * 1. Validates the MATCH_RECOGNIZE plan (duplicates, undefined variables, etc.)
 * 2. Creates a resolved MatchRecognizeAndMeasure plan with the measures in their
 *    resolved-but-not-rewritten form (still referencing virtualColumns ExprIds)
 *
 * The rewriting of measure expressions (mapping virtualColumns to child.output attributes
 * and adding classifier filters) is deferred to the physical planning phase in
 * MatchRecognizeStrategy, where the logical plan is decomposed into two physical operators.
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
    !m.virtualColumns.isEmpty &&
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
    // Check that MEASURES clause is not empty
    if (m.measures.isEmpty) {
      m.failAnalysis(
        errorClass = "MATCH_RECOGNIZE_EMPTY_MEASURES",
        messageParameters = Map.empty)
    }

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


    // Check that column references in measures are valid.
    // For ONE ROW PER MATCH (FINAL semantics), bare column references (qualified or
    // unqualified) outside aggregates are equivalent to LAST(col). They are auto-wrapped
    // with LAST in resolveMatchRecognize.
    // For ALL ROWS PER MATCH (RUNNING semantics), qualified column references are
    // only allowed inside aggregate functions.
    val partitionAttrSet = AttributeSet(m.virtualColumns.partitionAttrs)
    val virtualColumnSet = AttributeSet(m.virtualColumns.attrs)
    def checkValidMeasureExpression(expr: Expression): Unit = expr match {
      case ae: AggregateExpression =>
        // Validate nested aggregates and non-deterministic expressions
        ExprUtils.checkValidAggregateExpression(ae)
        // Check that all attributes in the aggregate have the same qualifier
        val attrs = ae.collect {
          case a: Attribute if virtualColumnSet.contains(a) => a
        }
        val qualifiers = attrs.map(_.qualifier).distinct
        if (qualifiers.size > 1) {
          m.failAnalysis(
            errorClass = "MATCH_RECOGNIZE_AGGREGATE_MIXED_QUALIFIERS",
            messageParameters = Map(
              "expression" -> toPrettySQL(ae),
              "qualifiers" -> qualifiers.map(_.mkString(".")).mkString(", ")))
        }
      case _: Attribute if !m.allRowsPerMatch =>
        // ONE ROW PER MATCH: all attrs are OK (auto-wrapped with LAST by the analyzer)
      case a: Attribute if partitionAttrSet.contains(a) =>
        // Partition attrs are always allowed outside aggregates
      case a: Attribute if virtualColumnSet.contains(a) =>
        // Non-partition virtual column attrs must be inside an aggregate
        m.failAnalysis(
          errorClass = "MATCH_RECOGNIZE_QUALIFIED_COLUMN_OUTSIDE_AGGREGATE",
          messageParameters = Map("expression" -> toPrettySQL(a)))
      case e => e.children.foreach(checkValidMeasureExpression)
    }
    m.measures.foreach(alias => checkValidMeasureExpression(alias.child))
  }

  private def resolveMatchRecognize(m: UnresolvedMatchRecognize): LogicalPlan = {
    // Validate the plan before transformation
    validateMatchRecognize(m)

    val measures = if (m.allRowsPerMatch) {
      m.measures
    } else {
      // ONE ROW PER MATCH: wrap bare column references with LAST().
      // Per Oracle/SQL standard, bare columns are equivalent to LAST(col).
      val partitionAttrSet = AttributeSet(m.virtualColumns.partitionAttrs)
      val virtualColumnSet = AttributeSet(m.virtualColumns.attrs)
      m.measures.map { alias =>
        alias.withNewChild(wrapBareAttrsWithLast(alias.child, partitionAttrSet, virtualColumnSet))
      }
    }

    MatchRecognizeAndMeasure(
      m.partitionSpec,
      m.orderSpec,
      m.pattern,
      m.patternVariableDefinitions,
      measures,
      m.allRowsPerMatch,
      m.virtualColumns,
      m.child
    )
  }

  /**
   * Wraps bare (non-aggregate, non-partition) attributes with LAST() for ONE ROW PER MATCH.
   * Attributes inside AggregateExpressions are left untouched.
   */
  private def wrapBareAttrsWithLast(
      expr: Expression,
      partitionAttrSet: AttributeSet,
      virtualColumnSet: AttributeSet): Expression = expr match {
    case _: AggregateExpression => expr
    case a: Attribute if virtualColumnSet.contains(a) && !partitionAttrSet.contains(a) =>
      new Last(a, ignoreNulls = false).toAggregateExpression()
    case other =>
      other.mapChildren(wrapBareAttrsWithLast(_, partitionAttrSet, virtualColumnSet))
  }
}
