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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, PLAN_EXPRESSION}
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Updates CTE references with the resolve output attributes of corresponding CTE definitions.
 */
object ResolveWithCTE extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (plan.containsAllPatterns(CTE)) {
      val cteDefMap = mutable.HashMap.empty[Long, CTERelationDef]
      resolveWithCTE(plan, cteDefMap)
    } else {
      plan
    }
  }

  private def resolveWithCTE(
      plan: LogicalPlan,
      cteDefMap: mutable.HashMap[Long, CTERelationDef]): LogicalPlan = {
    plan.resolveOperatorsDownWithPruning(_.containsAllPatterns(CTE)) {
      case withCTE @ WithCTE(_, cteDefs) =>
        val newCTEDefs = cteDefs.map {
          case cteDef if !cteDef.recursive =>
            if (cteDef.resolved) {
              cteDefMap.put(cteDef.id, cteDef)
            }
            cteDef
          case cteDef =>
            if (hasRecursiveCTERef(cteDef.child, cteDef.id)) {
              cteDef.child match {
                // If it's a supported recursive CTE query pattern, extract the anchor plan and
                // rewrite Union with UnionLoop. The recursive CTE references inside UnionLoop will
                // be rewritten as UnionLoopRef, using the output of the resolved anchor plan.
                case alias @ SubqueryAlias(_, Union(Seq(anchor, recursion), false, false)) =>
                  if (!anchor.resolved) {
                    cteDef
                  } else {
                    val loop = UnionLoop(
                      cteDef.id,
                      anchor,
                      rewriteRecursiveCTERefs(recursion, anchor, cteDef.id, None))
                    cteDef.copy(child = alias.copy(child = loop))
                  }

                case alias @ SubqueryAlias(_,
                    columnAlias @ UnresolvedSubqueryColumnAliases(
                      colNames,
                      Union(Seq(anchor, recursion), false, false)
                    )) =>
                  if (!anchor.resolved) {
                    cteDef
                  } else {
                    val loop = UnionLoop(
                      cteDef.id,
                      anchor,
                      rewriteRecursiveCTERefs(recursion, anchor, cteDef.id, Some(colNames)))
                    cteDef.copy(child = alias.copy(child = columnAlias.copy(child = loop)))
                  }

                // If the recursion is described with an UNION (deduplicating) clause then the
                // recursive term should not return those rows that have been calculated previously,
                // and we exclude those rows from the current iteration result.
                case alias @ SubqueryAlias(_,
                    Distinct(Union(Seq(anchor, recursion), false, false))) =>
                  if (!anchor.resolved) {
                    cteDef
                  } else {
                    val loop = UnionLoop(
                      cteDef.id,
                      Distinct(anchor),
                      Except(
                        rewriteRecursiveCTERefs(recursion, anchor, cteDef.id, None),
                        UnionLoopRef(cteDef.id, anchor.output, true),
                        isAll = false
                      )
                    )
                    cteDef.copy(child = alias.copy(child = loop))
                  }

                case alias @ SubqueryAlias(_,
                    columnAlias@UnresolvedSubqueryColumnAliases(
                      colNames,
                      Distinct(Union(Seq(anchor, recursion), false, false))
                    )) =>
                  if (!anchor.resolved) {
                    cteDef
                  } else {
                    val loop = UnionLoop(
                      cteDef.id,
                      Distinct(anchor),
                      Except(
                        rewriteRecursiveCTERefs(recursion, anchor, cteDef.id, Some(colNames)),
                        UnionLoopRef(cteDef.id, anchor.output, true),
                        isAll = false
                      )
                    )
                    cteDef.copy(child = alias.copy(child = columnAlias.copy(child = loop)))
                  }

                case other if !other.exists(_.isInstanceOf[UnionLoop]) =>
                  // We do not support cases of sole Union (needs a SubqueryAlias above it), nor
                  // Project (as UnresolvedSubqueryColumnAliases have not been substituted with the
                  // Project yet), leaving us with cases of SubqueryAlias->Union and SubqueryAlias->
                  // UnresolvedSubqueryColumnAliases->Union. The same applies to Distinct Union.
                  throw QueryCompilationErrors.recursiveCteError(
                    "Unsupported recursive CTE UNION placement.")
              }
            } else {
              if (cteDef.resolved) {
                cteDefMap.put(cteDef.id, cteDef)
              }
              cteDef
            }
        }
        withCTE.copy(cteDefs = newCTEDefs)

      case ref: CTERelationRef if !ref.resolved =>
        cteDefMap.get(ref.cteId).map { cteDef =>
          CTERelationRef(cteDef.id, cteDef.resolved, cteDef.output, cteDef.isStreaming)
        }.getOrElse {
          ref
        }

      case other =>
        other.transformExpressionsWithPruning(_.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
          case e: SubqueryExpression => e.withNewPlan(resolveWithCTE(e.plan, cteDefMap))
        }
    }
  }

  private def hasRecursiveCTERef(plan: LogicalPlan, cteId: Long): Boolean = plan match {
    case ref: CTERelationRef => ref.recursive && ref.cteId == cteId
    case other => other.expressions.filter(_.containsPattern(PLAN_EXPRESSION)).exists(_.exists {
      case e: SubqueryExpression => hasRecursiveCTERef(e.plan, cteId)
      case _ => false
    }) || other.children.exists(hasRecursiveCTERef(_, cteId))
  }

  // Substitute CTERelationRef with UnionLoopRef.
  private def rewriteRecursiveCTERefs(
      plan: LogicalPlan,
      anchor: LogicalPlan,
      cteId: Long,
      columnNames: Option[Seq[String]]) = {
    plan.transformWithPruning(_.containsPattern(CTE)) {
      case r: CTERelationRef if r.recursive && r.cteId == cteId =>
        val ref = UnionLoopRef(r.cteId, anchor.output, false)
        columnNames.map(UnresolvedSubqueryColumnAliases(_, ref)).getOrElse(ref)
    }
  }
}
