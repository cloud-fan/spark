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

package org.apache.spark.sql.catalyst.expressions

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.internal.SQLConf

/**
 * Utility class that represents a projection as in operators like Project.
 * This is used to map input properties such as constraints, ordering, partitioning, etc., to
 * corresponding output properties through alias replacement.
 */
case class ExpressionProjection(projectList: Seq[NamedExpression]) extends SQLConfHelper {
  // Skip alias replacement if `projectList` is bigger than this threshold
  private val projectListSizeThreshold =
    conf.getConf(SQLConf.MAX_ALIAS_REPLACEMENT_PROJECT_LIST_SIZE)

  // Stops further alias replacement once size of candidate expressions is bigger than this
  // threshold
  private val candidateExprSizeThreshold =
    conf.getConf(SQLConf.MAX_ALIAS_REPLACEMENT_PROJECTED_EXPRESSIONS_LIST_SIZE)

  // Projection output attributes
  private lazy val outputSet = AttributeSet(projectList.map(_.toAttribute))

  // A map of aliased expressions to a list of corresponding aliases.
  // For example, "a as x1, a as x2, b, b as y, c" => `a -> (x1, x2), b -> (y)`
  private lazy val projectMap = {
    val map = mutable.HashMap[Expression, ArrayBuffer[Attribute]]()
    projectList.foreach {
      case Alias(e, _) if e.references.isEmpty => // Nothing to be replaced.
      case a @ Alias(e, _) =>
        val attrs =
          map.getOrElseUpdate(e.canonicalized, ArrayBuffer[Attribute]())
        attrs.append(a.toAttribute)
      case _ => // No alias to replace with.
    }
    map
  }

  // A map of each expression to the number of other aliased expressions it contains. Aliased
  // expressions that do not contain any other aliased expression will not be in this map.
  // For example,
  // 1. "a, c as x, a + b as y, a + b + 1 as z" => `(a + b) -> 1, (a + b + 1) -> 2`
  // 2. "a, a as w, b as x, a + b as y, a + b + 1 as z"  => `(a + b) -> 2, (a + b + 1) -> 3`
  private lazy val dependency = {
    val map = mutable.HashMap[Expression, Int]()
    val attributeRefSet = projectList.collect {
      case e: Attribute => e
    }.toSet
    val keySet = projectMap.keySet
    val depSet = keySet ++ attributeRefSet
    keySet.foreach { k =>
      val dep = depSet.collect {
        case e if !k.semanticEquals(e) && k.find(_.semanticEquals(e)).isDefined => e
      }.size
      if (dep > 0) {
        map.put(k, dep)
      }
    }
    map
  }

  // Aliased expressions in counter topological order.
  // For example, "a as x, a + b as y, a + b + 1 as z" => `(a + b + 1), (a + b), a`
  private lazy val sortedAliasedExpressions = {
    projectMap.keys.toSeq.sortWith { (x, y) =>
      val depX = dependency.get(x)
      val depY = dependency.get(y)
      depY.isEmpty || (depX.isDefined && depX.get > depY.get)
    }
  }

  /**
   * Transform this expression by replacing aliased expressions with their corresponding aliases.
   * When multiple aliases associate with the same expression, combinations of replacement with
   * all these aliases will be returned.
   *
   * Examples:
   * 1) For aliased expressions as "a as x, b as y",
   *    `a + b` generates `Seq(x + y)`;
   * 2) For aliased expression as "a as x1, a as x2, b as y"
   *    `a + b` generates `(x1 + y, x2 + y)`;
   * 3) For aliased expression as "a as x, b as y, a + b as z"
   *    `a + b` generates `(x + y, z)`;
   * 4) For aliased expression as "a as x"
   *    `a + b` generates `()`, since `b` is not found in the aliased expression output;
   * 5) For aliased expression as "a, b as y"
   *    `a + b` generates `(a + y)`.
   *
   * @param expression the [[Expression]] to be transformed.
   * @return the replaced [[Expression]]s, or an empty list if this expression contains attribute
   *         references that cannot be replaced by an alias.
   */
  def replaceWithAlias(expression: Expression): Seq[Expression] = {
    // Start with the candidate list `projectedExprs` containing the original `expression` and
    // apply replacement of the aliased expressions in counter topological order (e.g., given
    // "a + b as x, a as y", replace `a + b` with "x" first). Note that this replacement routine
    // only applies to the aliased expressions, but not attribute-reference expressions projected
    // without alias. For example, given "a, a as x, b as y", the routine will only test the
    // presence of the non-alias projection "a", but the actual replacement will only be attempted
    // for "a as x" and "b as y".
    // For each replacement of an aliased expression `k` that maps to aliases `A1, A2, ..., An`:
    //   1. For each expression `e` in the candidate list `projectedExprs`, if `k` is equivalent
    //      or is a sub-expression of `e`, then emit replaced expressions for all aliases:
    //      `replace(e, k -> A1), replace(e, k -> A2), ..., replace(e, k -> An)`.
    //   2. Add all replaced expressions from step 1 into the candidate list `projectedExprs`.
    //   3. Trim the candidate list `projectedExprs`:
    //      If `k` does not contain another aliased expression (i.e., `!dependency.contains(k)`),
    //      and `k` does not have a non-alias attribute-reference projection, then remove
    //      expressions containing `k` from the candidate list; otherwise it means either
    //      1) `k` has a non-alias attribute-reference projection alongside aliased projection
    //        （e.g., "k, k as A1"), then `k` will be part of the output attributes itself, thus it
    //         should not be removed from the candidate list; or
    //      2) there are other aliased sub-expressions of `k`, and candidates containing `k` can
    //         be replaced by the aliases of those sub-expressions later on.
    //      This step can be omitted if no match is found for `k` in step 1.
    // Note that step 3 is important for performance, because otherwise the candidate list could
    // expand dramatically in size in some cases and eventually cause memory issues.
    // Expressions projected as an attribute reference only and with no aliases will not go through
    // the above replacement logic at all.
    var projectedExprs = Seq(expression)
    if (projectList.size <= projectListSizeThreshold) {
      // After the size of projectedExprs hits the candidate expressions size threshold, this for
      // loop will break. This stops further alias replacement.
      for (k <- sortedAliasedExpressions if projectedExprs.size < candidateExprSizeThreshold) {
        val exprs = projectMap(k).flatMap { attr =>
          projectedExprs.flatMap { expr =>
            val newExpr = expr.transformDown {
              case expr: Expression if expr.semanticEquals(k) =>
                attr
            }
            if (newExpr != expr) Some(newExpr) else None
          }
        }
        lazy val hasNonAliasProjection =
          (k.isInstanceOf[Attribute]
            && outputSet.contains(k.asInstanceOf[Attribute]))
        if (exprs.nonEmpty && !hasNonAliasProjection && !dependency.contains(k)) {
          projectedExprs = projectedExprs.filter(e => e.find(_.semanticEquals(k)).isEmpty)
        }
        projectedExprs ++= exprs
      }
    }

    // Filter out expressions that contain attributes that do not exist in the projected output.
    projectedExprs.filter { c =>
      c.references.subsetOf(outputSet)
    }
  }
}
