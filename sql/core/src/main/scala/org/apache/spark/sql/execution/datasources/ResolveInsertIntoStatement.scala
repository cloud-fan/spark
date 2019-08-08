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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalog.v2.expressions.{FieldReference, IdentityTransform}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedV2Table}
import org.apache.spark.sql.catalyst.catalog.UnresolvedCatalogRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Cast, EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, InsertIntoTable, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic, Project}
import org.apache.spark.sql.catalyst.plans.logical.sql.InsertIntoStatement
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.sources.v2.Table

/**
 * A rule to resolve [[InsertIntoStatement]], by converting it to either v2 writing commands, or v1
 * [[InsertIntoTable]].
 */
class ResolveInsertIntoStatement(conf: SQLConf) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case i @ InsertIntoStatement(relation: DataSourceV2Relation, _, _, _, _) if i.query.resolved =>
      // wait for the `DataSourceResolution` rule to resolve `UnresolvedV2Table`.
      if (relation.table.isInstanceOf[UnresolvedV2Table]) return i

      // ifPartitionNotExists is append with validation, but validation is not supported
      if (i.ifPartitionNotExists) {
        throw new AnalysisException(
          s"Cannot write, IF NOT EXISTS is not supported for table: ${relation.table.name}")
      }

      val partCols = partitionColumnNames(relation.table)
      validatePartitionSpec(partCols, i.partitionSpec)

      val staticPartitions = i.partitionSpec.filter(_._2.isDefined).mapValues(_.get)
      val query = addStaticPartitionColumns(relation, i.query, staticPartitions)
      val dynamicPartitionOverwrite = partCols.size > staticPartitions.size &&
        conf.partitionOverwriteMode == PartitionOverwriteMode.DYNAMIC

      if (!i.overwrite) {
        AppendData.byPosition(relation, query)
      } else if (dynamicPartitionOverwrite) {
        OverwritePartitionsDynamic.byPosition(relation, query)
      } else {
        OverwriteByExpression.byPosition(
          relation, query, staticDeleteExpression(relation, staticPartitions))
      }

    case i @ InsertIntoStatement(u: UnresolvedCatalogRelation, _, _, _, _) if i.query.resolved =>
      InsertIntoTable(u, i.partitionSpec, i.query, i.overwrite, i.ifPartitionNotExists)
  }

  private def partitionColumnNames(table: Table): Seq[String] = {
    // get partition column names. in v2, partition columns are columns that are stored using an
    // identity partition transform because the partition values and the column values are
    // identical. otherwise, partition values are produced by transforming one or more source
    // columns and cannot be set directly in a query's PARTITION clause.
    table.partitioning.flatMap {
      case IdentityTransform(FieldReference(Seq(name))) => Some(name)
      case _ => None
    }
  }

  private def validatePartitionSpec(
      partitionColumnNames: Seq[String],
      partitionSpec: Map[String, Option[String]]): Unit = {
    // check that each partition name is a partition column. otherwise, it is not valid
    partitionSpec.keySet.foreach { partitionName =>
      partitionColumnNames.find(name => conf.resolver(name, partitionName)) match {
        case Some(_) =>
        case None =>
          throw new AnalysisException(
            s"PARTITION clause cannot contain a non-partition column name: $partitionName")
      }
    }
  }

  private def addStaticPartitionColumns(
      relation: DataSourceV2Relation,
      query: LogicalPlan,
      staticPartitions: Map[String, String]): LogicalPlan = {
    if (staticPartitions.isEmpty) {
      query
    } else {
      // add any static value as a literal column
      val withStaticPartitionValues = {
        // for each static name, find the column name it will replace and check for unknowns.
        val outputNameToStaticName = staticPartitions.keySet.map(staticName =>
          relation.output.find(col => conf.resolver(col.name, staticName)) match {
            case Some(attr) =>
              attr.name -> staticName
            case _ =>
              throw new AnalysisException(
                s"Cannot add static value for unknown column: $staticName")
          }).toMap

        val queryColumns = query.output.iterator

        // for each output column, add the static value as a literal, or use the next input
        // column. this does not fail if input columns are exhausted and adds remaining columns
        // at the end. both cases will be caught by ResolveOutputRelation and will fail the
        // query with a helpful error message.
        relation.output.flatMap { col =>
          outputNameToStaticName.get(col.name).flatMap(staticPartitions.get) match {
            case Some(staticValue) =>
              Some(Alias(Cast(Literal(staticValue), col.dataType), col.name)())
            case _ if queryColumns.hasNext =>
              Some(queryColumns.next)
            case _ =>
              None
          }
        } ++ queryColumns
      }

      Project(withStaticPartitionValues, query)
    }
  }

  private def staticDeleteExpression(
      relation: DataSourceV2Relation,
      staticPartitions: Map[String, String]): Expression = {
    if (staticPartitions.isEmpty) {
      Literal(true)
    } else {
      staticPartitions.map { case (name, value) =>
        relation.output.find(col => conf.resolver(col.name, name)) match {
          case Some(attr) =>
            // the delete expression must reference the table's column names, but these attributes
            // are not available when CheckAnalysis runs because the relation is not a child of
            // the logical operation. instead, expressions are resolved after
            // ResolveOutputRelation runs, using the query's column names that will match the
            // table names at that point. because resolution happens after a future rule, create
            // an UnresolvedAttribute.
            EqualTo(UnresolvedAttribute(attr.name), Cast(Literal(value), attr.dataType))
          case None =>
            throw new AnalysisException(s"Unknown static partition column: $name")
        }
      }.reduce(And)
    }
  }
}
