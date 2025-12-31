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
 * Current implementation supports:
 * - Mode: ONE ROW PER MATCH with AFTER MATCH SKIP PAST LAST ROW
 * - Pattern: Sequences with quantifiers (+, *, ?, {n}, {n,m}) and alternation (|)
 * - Measures: Basic expressions using columns from the input
 *
 * @param partitionSpec Named expressions used to partition the input data
 * @param orderSpec Sort order within each partition
 * @param pattern The pattern expression AST defining the pattern to match
 * @param patternVariableDefinitions Definitions for pattern variables as Alias (DEFINE clause)
 * @param measures Output measure expressions as Alias (MEASURES clause)
 * @param matchedRowsAttrs Attributes available for MEASURES resolution. Created during analysis
 *                         with unique ExprIds for each PATTERN variable qualifier.
 *                         This includes child output attributes and PATTERN variable qualified
 *                         versions (e.g., A.price, B.price each get distinct ExprIds).
 *                         Starts as Nil at parse time.
 * @param child The child logical plan
 */
case class UnresolvedMatchRecognize(
    partitionSpec: Seq[NamedExpression],
    orderSpec: Seq[SortOrder],
    pattern: RowPattern,
    patternVariableDefinitions: Seq[Alias],
    measures: Seq[Alias],
    matchedRowsAttrs: Seq[AttributeReference],
    child: LogicalPlan) extends UnaryNode {

  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_MATCH_RECOGNIZE)

  // Output is strictly the MEASURES columns (ONE ROW PER MATCH semantics).
  // Users can include partition columns in MEASURES if needed.
  override def output: Seq[Attribute] = measures.map(_.toAttribute)

  // Produced attributes include matchedRowsAttrs (partition attrs + matched row attrs).
  // Partition attrs are at the beginning of matchedRowsAttrs.
  override def producedAttributes: AttributeSet = AttributeSet(matchedRowsAttrs)

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
 * Resolved logical plan for MATCH_RECOGNIZE pattern matching.
 *
 * This operator processes sorted partitions to find pattern matches and produces
 * matched rows with match number and classifier columns. A MatchRecognizeMeasures node
 * is added on top to aggregate and produce the final measures.
 *
 * Similar to the physical MatchRecognizeExec, this plan outputs:
 * - Partition columns
 * - Match number attribute (starts with 1 and increases for each match within a partition)
 * - Classifier attribute (pattern variable name for each matched row)
 * - Child output (columns from matched rows)
 *
 * @param partitionSpec Named expressions for partitioning the input data
 * @param orderSpec Sort order within each partition
 * @param pattern The pattern expression AST (e.g., PatternSequence for "A B C")
 * @param patternVariableDefinitions Definitions for pattern variables (DEFINE clause)
 * @param matchNumberAttr Internal attribute for match number (starts with 1, increases per match)
 * @param classifierAttr Internal attribute for pattern variable name per matched row
 * @param child The child logical plan
 */
case class MatchRecognize(
    partitionSpec: Seq[NamedExpression],
    orderSpec: Seq[SortOrder],
    pattern: RowPattern,
    patternVariableDefinitions: Seq[Alias],
    matchNumberAttr: AttributeReference,
    classifierAttr: AttributeReference,
    child: LogicalPlan) extends UnaryNode {

  final override val nodePatterns: Seq[TreePattern] = Seq(MATCH_RECOGNIZE)

  private val partitionOutputAttrs = partitionSpec.map(_.toAttribute)

  // Matched row attributes derived from child output
  private val matchedRowsAttrs = child.output

  // Output includes partition columns, internal columns (match_number, classifier),
  // then matched row columns
  override def output: Seq[Attribute] =
    partitionOutputAttrs ++ Seq(matchNumberAttr, classifierAttr) ++ matchedRowsAttrs

  // These attributes are produced by this operator, not expected from the child
  override def producedAttributes: AttributeSet =
    AttributeSet(Seq(matchNumberAttr, classifierAttr))

  override protected def withNewChildInternal(newChild: LogicalPlan): MatchRecognize =
    copy(child = newChild)
}

/**
 * Logical plan for aggregating MATCH_RECOGNIZE results to produce measures.
 *
 * This node sits on top of MatchRecognize and applies aggregation to produce
 * the final measure expressions. It groups by partition columns + match number
 * and computes the aggregate measures.
 *
 * For aggregate expressions that reference PATTERN variable qualified attributes
 * (e.g., LAST(A.price)), they are rewritten to use original child.output attributes
 * with a classifier filter (e.g., LAST(price) FILTER (WHERE _classifier = 'A')).
 *
 * For PATTERN variable qualified column references appearing alone (not in aggregate),
 * they are wrapped with LAST function and classifier filter.
 *
 * @param groupingExprs Partition columns + match number for grouping
 * @param measures Output measure expressions as Alias (already rewritten with classifier filters)
 * @param child The child MatchRecognize plan
 */
case class MatchRecognizeMeasures(
    groupingExprs: Seq[Attribute],
    measures: Seq[Alias],
    child: LogicalPlan) extends UnaryNode {

  final override val nodePatterns: Seq[TreePattern] = Seq(MATCH_RECOGNIZE_MEASURES)

  // Output is strictly the MEASURES columns (ONE ROW PER MATCH semantics).
  // Users can include partition columns in MEASURES if needed.
  override def output: Seq[Attribute] = measures.map(_.toAttribute)

  override protected def withNewChildInternal(newChild: LogicalPlan): MatchRecognizeMeasures =
    copy(child = newChild)
}
