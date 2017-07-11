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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BoundReference, ExpressionSet}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.execution.{ColumnarBatchScan, LeafExecNode, WholeStageCodegenExec}
import org.apache.spark.sql.sources.v2.reader.{ColumnarReadSupport, DataSourceV2Reader}

case class DataSourceV2ScanExec(
    fullOutput: Array[AttributeReference],
    @transient reader: DataSourceV2Reader,
    // TODO: these 3 parameters are only used to determine the equality of the scan node, however,
    // the reader also have this information, and ideally we can just rely on the equality of the
    // reader. The only concern is, the reader implementation is outside of Spark and we have no
    // control.
    requiredColumnsIndex: Seq[Int],
    @transient filters: ExpressionSet,
    hashPartitionKeys: Seq[String]) extends LeafExecNode with ColumnarBatchScan {

  def output: Seq[Attribute] = requiredColumnsIndex.map(fullOutput)

  override protected def doExecute(): RDD[InternalRow] = reader match {
    case r: ColumnarReadSupport if r.supportsColumnarReads() =>
      WholeStageCodegenExec(this).execute()
    case _ =>
      val numOutputRows = longMetric("numOutputRows")
      inputRDD.map { r =>
        numOutputRows += 1
        r
      }
  }

  private lazy val inputRDD: RDD[InternalRow] = {
    reader match {
      case r: ColumnarReadSupport if r.supportsColumnarReads() =>
        new DataSourceRDD(sparkContext, r.createColumnarReadTasks())
          .asInstanceOf[RDD[InternalRow]]
      case _ =>
        new DataSourceRDD(sparkContext, reader.createUnsafeRowReadTasks())
          .asInstanceOf[RDD[InternalRow]]
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = Seq(inputRDD)

  override protected def doProduce(ctx: CodegenContext): String = {
    if (reader.isInstanceOf[ColumnarReadSupport]) {
      return super.doProduce(ctx)
    }
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    // PhysicalRDD always just has one input
    val input = ctx.freshName("input")
    ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")
    val exprRows = output.zipWithIndex.map{ case (a, i) =>
      BoundReference(i, a.dataType, a.nullable)
    }
    val row = ctx.freshName("row")
    ctx.INPUT_ROW = row
    ctx.currentVars = null
    val columnsRowInput = exprRows.map(_.genCode(ctx))
    s"""
       |while ($input.hasNext()) {
       |  InternalRow $row = (InternalRow) $input.next();
       |  $numOutputRows.add(1);
       |  ${consume(ctx, columnsRowInput, row).trim}
       |  if (shouldStop()) return;
       |}
     """.stripMargin
  }
}
