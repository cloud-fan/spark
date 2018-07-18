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

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartitionReader, InputSplit, SplitReaderProvider}

class DataSourceRDDPartition(val index: Int, val inputSplit: InputSplit)
  extends Partition with Serializable

// TODO: we should have 2 RDDs: an RDD[InternalRow] for row-based scan, an `RDD[ColumnarBatch]` for
// columnar scan.
class DataSourceRDD(
    sc: SparkContext,
    @transient private val inputSplits: Seq[InputSplit],
    splitReaderProvider: SplitReaderProvider)
  extends RDD[InternalRow](sc, Nil) {

  override protected def getPartitions: Array[Partition] = {
    inputSplits.zipWithIndex.map {
      case (split, index) => new DataSourceRDDPartition(index, split)
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val inputSplit = split.asInstanceOf[DataSourceRDDPartition].inputSplit
    val reader: InputPartitionReader[_] = if (splitReaderProvider.supportColumnarReader()) {
      splitReaderProvider.createColumnarReader(inputSplit)
    } else {
      splitReaderProvider.createRowReader(inputSplit)
    }
    context.addTaskCompletionListener(_ => reader.close())
    val iter = new Iterator[Any] {
      private[this] var valuePrepared = false

      override def hasNext: Boolean = {
        if (!valuePrepared) {
          valuePrepared = reader.next()
        }
        valuePrepared
      }

      override def next(): Any = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        valuePrepared = false
        reader.get()
      }
    }
    // TODO: get rid of this type hack.
    new InterruptibleIterator(context, iter.asInstanceOf[Iterator[InternalRow]])
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[DataSourceRDDPartition].inputSplit.preferredLocations()
  }
}
