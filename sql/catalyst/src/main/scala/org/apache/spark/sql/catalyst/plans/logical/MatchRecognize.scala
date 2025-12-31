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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.trees.TreePattern._

/**
 * Unresolved logical plan for the MATCH_RECOGNIZE operator.
 *
 * MATCH_RECOGNIZE is used for pattern matching on ordered rows. It finds sequences
 * of rows that match a specified pattern and extracts measures from the matched rows.
 *
 * SQL syntax (simplified):
 * {{{
 *   SELECT ...
 *   FROM table
 *   MATCH_RECOGNIZE (
 *     PARTITION BY partition_cols
 *     ORDER BY order_cols
 *     MEASURES measure_exprs AS measure_names
 *     ONE ROW PER MATCH
 *     AFTER MATCH SKIP PAST LAST ROW
 *     PATTERN (pattern_expr)
 *     DEFINE pattern_var_definitions
 *   )
 * }}}
 *
 * @param partitionSpec Named expressions used to partition the input data
 * @param orderSpec Sort order within each partition
 * @param pattern The pattern expression AST defining the pattern to match
 * @param patternVariableDefinitions Definitions for pattern variables as Alias (DEFINE clause)
 * @param measures Output measure expressions as Alias (MEASURES clause)
 * @param allRowsPerMatch If true, ALL ROWS PER MATCH (output every matched row with running
 *                        measures). If false, ONE ROW PER MATCH (default, one summary row).
 * @param virtualColumns Virtual column layout for MEASURES resolution. Created during analysis
 *                       with unique ExprIds for each PATTERN variable qualifier.
 *                       This includes child output attributes and PATTERN variable qualified
 *                       versions (e.g., A.price, B.price each get distinct ExprIds).
 *                       Starts as empty at parse time.
 * @param child The child logical plan
 */
case class UnresolvedMatchRecognize(
    partitionSpec: Seq[NamedExpression],
    orderSpec: Seq[SortOrder],
    pattern: RowPattern,
    patternVariableDefinitions: Seq[Alias],
    measures: Seq[Alias],
    allRowsPerMatch: Boolean,
    virtualColumns: MatchRecognizeVirtualColumns,
    child: LogicalPlan) extends UnaryNode {

  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_MATCH_RECOGNIZE)

  // ONE ROW PER MATCH: PARTITION BY columns + MEASURES columns
  // ALL ROWS PER MATCH: all input columns + MEASURES columns
  override def output: Seq[Attribute] = {
    if (allRowsPerMatch) {
      child.output ++ measures.map(_.toAttribute)
    } else {
      partitionSpec.map(_.toAttribute) ++ measures.map(_.toAttribute)
    }
  }

  override def producedAttributes: AttributeSet = AttributeSet(virtualColumns.attrs)

  // Always unresolved to prevent analyzer from resolving downstream operators.
  // ResolveMatchRecognize will check specific conditions before transforming.
  override lazy val resolved: Boolean = false

  /**
   * Returns a map from variable name to condition for efficient lookup.
   */
  lazy val variableConditions: Map[String, Expression] =
    patternVariableDefinitions.map(a => a.name -> a.child).toMap

  /**
   * Validates that all variables referenced in the pattern are defined.
   */
  lazy val undefinedVariables: Set[String] =
    pattern.variableNames -- patternVariableDefinitions.map(_.name).toSet

  override protected def withNewChildInternal(newChild: LogicalPlan): UnresolvedMatchRecognize =
    copy(child = newChild)
}

/**
 * Resolved logical plan for the MATCH_RECOGNIZE operator.
 *
 * This single logical plan represents the complete MATCH_RECOGNIZE operation:
 * pattern matching followed by measure computation. The physical planner
 * decomposes it into physical operators depending on the output mode:
 * - ONE ROW PER MATCH: MatchRecognizeExec + SortAggregateExec
 * - ALL ROWS PER MATCH: MatchRecognizeExec + WindowExec + ProjectExec
 *
 * @param partitionSpec Named expressions for partitioning the input data
 * @param orderSpec Sort order within each partition
 * @param pattern The pattern expression AST (e.g., PatternSequence for "A B C")
 * @param patternVariableDefinitions Definitions for pattern variables (DEFINE clause)
 * @param measures Output measure expressions as Alias (referencing virtualColumns)
 * @param allRowsPerMatch If true, ALL ROWS PER MATCH; if false, ONE ROW PER MATCH
 * @param virtualColumns Virtual column layout from the resolution phase with unique ExprIds
 *                       per pattern variable qualifier. Used by the physical planner to build
 *                       the attribute mapping for measure rewriting.
 * @param child The child logical plan
 */
case class MatchRecognizeAndMeasure(
    partitionSpec: Seq[NamedExpression],
    orderSpec: Seq[SortOrder],
    pattern: RowPattern,
    patternVariableDefinitions: Seq[Alias],
    measures: Seq[Alias],
    allRowsPerMatch: Boolean,
    virtualColumns: MatchRecognizeVirtualColumns,
    child: LogicalPlan) extends UnaryNode {

  final override val nodePatterns: Seq[TreePattern] = Seq(MATCH_RECOGNIZE)

  // ONE ROW PER MATCH: PARTITION BY columns + MEASURES columns
  // ALL ROWS PER MATCH: all input columns + MEASURES columns
  override def output: Seq[Attribute] = {
    if (allRowsPerMatch) {
      child.output ++ measures.map(_.toAttribute)
    } else {
      partitionSpec.map(_.toAttribute) ++ measures.map(_.toAttribute)
    }
  }

  override def producedAttributes: AttributeSet = AttributeSet(virtualColumns.attrs)

  override protected def withNewChildInternal(
      newChild: LogicalPlan): MatchRecognizeAndMeasure =
    copy(child = newChild)
}

/**
 * Virtual column layout for MATCH_RECOGNIZE, used in both analysis and planning.
 *
 * Layout: [partition attrs] + [unqualified child attrs] + [per-variable qualified attrs]
 *
 * Each group of qualified attrs has the same size as the child output, with the qualifier
 * set to the pattern variable name.
 *
 * @param attrs All virtual column attributes
 * @param numPartitionAttrs Number of leading partition attributes
 * @param numChildAttrs Number of child output attributes (size of each unqualified/qualified group)
 */
case class MatchRecognizeVirtualColumns(
    attrs: Seq[AttributeReference],
    numPartitionAttrs: Int,
    numChildAttrs: Int) {

  def isEmpty: Boolean = attrs.isEmpty

  lazy val partitionAttrs: Seq[AttributeReference] =
    attrs.take(numPartitionAttrs)

  lazy val unqualifiedAttrs: Seq[AttributeReference] =
    attrs.slice(numPartitionAttrs, numPartitionAttrs + numChildAttrs)

  def qualifiedAttrsByVar(numPatternVars: Int): Seq[Seq[AttributeReference]] = {
    (0 until numPatternVars).map { idx =>
      val start = numPartitionAttrs + numChildAttrs + idx * numChildAttrs
      attrs.slice(start, start + numChildAttrs)
    }
  }

  lazy val allQualifiedAttrs: Seq[AttributeReference] =
    attrs.drop(numPartitionAttrs + numChildAttrs)
}

object MatchRecognizeVirtualColumns {
  val empty: MatchRecognizeVirtualColumns = MatchRecognizeVirtualColumns(Nil, 0, 0)
}
