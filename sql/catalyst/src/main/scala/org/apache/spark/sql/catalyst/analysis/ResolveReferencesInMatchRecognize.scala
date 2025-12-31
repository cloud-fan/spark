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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, UnresolvedMatchRecognize}
import org.apache.spark.sql.connector.catalog.CatalogManager

/**
 * Resolves column references in MATCH_RECOGNIZE.
 *
 * Column resolution in MATCH_RECOGNIZE MEASURES is special because:
 * 1. Partition columns are directly available (like grouping columns in aggregation)
 * 2. Columns from matched rows are in a special scope, not the child relation,
 *    so qualifiers from child.output should be dropped.
 * 3. Pattern variable names (A, B, C, etc.) can be used as qualifiers to refer
 *    to columns from specific matched rows.
 *
 * Resolution strategy for MEASURES:
 * 1. First try to resolve against partition columns (with qualifiers erased)
 * 2. Then try unqualified matched row attributes (with fresh ExprIds)
 * 3. Then try qualified versions for each pattern variable (e.g., `a.price`, `b.price`)
 *
 * Other expressions (partitionSpec, orderSpec, patternVariableDefinitions) resolve
 * against child.output directly using normal resolution.
 */
class ResolveReferencesInMatchRecognize(val catalogManager: CatalogManager)
    extends SQLConfHelper with ColumnResolutionHelper {

  def apply(m: UnresolvedMatchRecognize): UnresolvedMatchRecognize = {
    if (!m.child.resolved) return m
    // Return early if already fully resolved with matchedRowsAttrs populated
    if (m.resolved && m.matchedRowsAttrs.nonEmpty) return m

    // Resolve partition spec against child output directly
    val resolvedPartitionSpec = m.partitionSpec.map { expr =>
      resolveExpressionByPlanOutput(expr, m.child).asInstanceOf[NamedExpression]
    }

    val resolvedOrderSpec = m.orderSpec.map { sortOrder =>
      resolveExpressionByPlanOutput(sortOrder, m.child).asInstanceOf[SortOrder]
    }

    // Resolve pattern variable definitions against child output directly
    val resolvedDefinitions = m.patternVariableDefinitions.map { alias =>
      resolveExpressionByPlanOutput(alias, m.child).asInstanceOf[Alias]
    }

    // Return early if partition spec, order spec, or definitions are not fully resolved yet.
    // This can happen when expressions reference columns that haven't been resolved.
    if (!resolvedPartitionSpec.forall(_.resolved) ||
        !resolvedOrderSpec.forall(_.resolved) ||
        !resolvedDefinitions.forall(_.resolved)) {
      return m.copy(
        partitionSpec = resolvedPartitionSpec,
        orderSpec = resolvedOrderSpec,
        patternVariableDefinitions = resolvedDefinitions
      )
    }

    // Build matchedRowsAttrs with unique ExprIds for each qualifier.
    // Only build once (when matchedRowsAttrs is empty) to avoid infinite loop.
    val patternVarNames = resolvedDefinitions.map(_.name)
    val childAttrs = m.child.output.map(_.asInstanceOf[AttributeReference])

    // Create partition attrs with fresh ExprIds to be stable across optimizer rewrites.
    // For Alias (e.g., PARTITION BY a + b AS col), toAttribute already has a unique ExprId.
    // For AttributeReference (e.g., PARTITION BY symbol), we call newInstance() to get
    // a fresh ExprId that won't be affected by optimizer attribute rewrites.
    val partitionAttrs = resolvedPartitionSpec.map { ne =>
      val attr = ne.toAttribute.asInstanceOf[AttributeReference]
      ne match {
        case _: Alias => attr.withQualifier(Seq.empty)
        case _ => attr.newInstance().withQualifier(Seq.empty)
      }
    }

    val matchedRowsAttrs = if (m.matchedRowsAttrs.isEmpty) {
      // Include partition attrs first, then matched row attrs
      partitionAttrs ++ buildMatchedRowsAttrs(childAttrs, patternVarNames)
    } else {
      m.matchedRowsAttrs
    }

    // Create multiple LocalRelations to avoid column name conflicts:
    // 1. Partition output attributes (with fresh ExprIds, no qualifiers)
    //    For PARTITION BY a + b AS col, only col is available, not a + b
    // 2. Unqualified matched row references
    // 3. One for each pattern variable qualifier
    val numPartitionAttrs = partitionAttrs.length
    val numChildAttrs = childAttrs.length
    val unqualifiedStart = numPartitionAttrs
    val unqualifiedEnd = numPartitionAttrs + numChildAttrs
    val unqualifiedAttrs = matchedRowsAttrs.slice(unqualifiedStart, unqualifiedEnd)
    val qualifiedAttrsByVar = patternVarNames.zipWithIndex.map { case (_, idx) =>
      val start = numPartitionAttrs + numChildAttrs + idx * numChildAttrs
      matchedRowsAttrs.slice(start, start + numChildAttrs)
    }

    // Partition columns first (they are directly available like grouping columns),
    // then unqualified matched rows, then qualified by pattern variable
    val resolutionPlans = LocalRelation(partitionAttrs) +:
      LocalRelation(unqualifiedAttrs) +: qualifiedAttrsByVar.map(LocalRelation(_))

    // Resolve measures against the LocalRelations one by one
    val resolvedMeasures = m.measures.map { alias =>
      resolveMeasureExpression(alias, resolutionPlans).asInstanceOf[Alias]
    }

    m.copy(
      partitionSpec = resolvedPartitionSpec,
      orderSpec = resolvedOrderSpec,
      patternVariableDefinitions = resolvedDefinitions,
      measures = resolvedMeasures,
      matchedRowsAttrs = matchedRowsAttrs
    )
  }

  /**
   * Builds the attributes available for MEASURES resolution.
   *
   * Creates attributes with unique ExprIds for:
   * 1. Unqualified child output (no qualifier)
   * 2. Each pattern variable as qualifier (e.g., a.price, b.price get distinct ExprIds)
   *
   * This allows the optimizer to correctly distinguish A.price from B.price
   * since they have different ExprIds.
   */
  private def buildMatchedRowsAttrs(
      childOutput: Seq[AttributeReference],
      patternVarNames: Seq[String]): Seq[AttributeReference] = {
    // 1. Unqualified output - new ExprIds, no qualifier
    val unqualifiedAttrs = childOutput.map { attr =>
      attr.newInstance().withQualifier(Seq.empty)
    }

    // 2. One set of attributes per pattern variable name
    val qualifiedAttrs = patternVarNames.flatMap { varName =>
      childOutput.map { attr =>
        attr.newInstance().withQualifier(Seq(varName))
      }
    }

    unqualifiedAttrs ++ qualifiedAttrs
  }

  /**
   * Resolves a measure expression against multiple LocalRelations one by one.
   * Each LocalRelation contains attributes for one qualifier to avoid name conflicts.
   * Stops early if the expression becomes fully resolved.
   */
  private def resolveMeasureExpression(
      expr: Expression,
      plans: Seq[LogicalPlan]): Expression = {
    var currentExpr = expr
    for (plan <- plans if !currentExpr.resolved) {
      currentExpr = resolveExpressionByPlanOutput(currentExpr, plan)
    }
    currentExpr
  }
}
