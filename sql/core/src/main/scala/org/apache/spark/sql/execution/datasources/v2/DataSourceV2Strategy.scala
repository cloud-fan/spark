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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources.v2.reader.{CatalystFilterPushDownSupport, ColumnPruningSupport, FilterPushDownSupport}

object DataSourceV2Strategy extends Strategy {
  // TODO: write path
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(projects, filters, DataSourceV2Relation(output, reader)) =>
      val attrMap = AttributeMap(output.zip(output))

      val projectSet = AttributeSet(projects.flatMap(_.references))
      val filterSet = AttributeSet(filters.flatMap(_.references))

      // Match original case of attributes.
      val requiredColumns = (projectSet ++ filterSet).toSeq.map(attrMap)
      val supportColumnPruning = reader match {
        case r: ColumnPruningSupport =>
          r.pruneColumns(requiredColumns.toStructType)
        case _ => false
      }

      val stayUpFilters = ListBuffer.empty[Expression]
      reader match {
        case r: CatalystFilterPushDownSupport =>
          for (filter <- filters) {
            if (!r.pushDownCatalystFilter(filter)) {
              stayUpFilters += filter
            }
          }
        case r: FilterPushDownSupport =>
          for (filter <- filters) {
            val publicFilter = DataSourceStrategy.translateFilter(filter)
            if (publicFilter.isEmpty) {
              stayUpFilters += filter
            } else if (!r.pushDownFilter(publicFilter.get)) {
              stayUpFilters += filter
            }
          }
        case _ =>
      }

      val scan = DataSourceV2ScanExec(
        output.toArray,
        reader,
        if (supportColumnPruning) requiredColumns.map(output.indexOf) else output.indices,
        ExpressionSet(filters),
        Nil)

      val filterCondition = filters.reduceLeftOption(And)
      val withFilter = filterCondition.map(FilterExec(_, scan)).getOrElse(scan)

      val withProject = if (projects == withFilter.output) {
        withFilter
      } else {
        ProjectExec(projects, withFilter)
      }

      // TODO: support hash partitioning push down.

      withProject :: Nil

    case _ => Nil
  }
}
