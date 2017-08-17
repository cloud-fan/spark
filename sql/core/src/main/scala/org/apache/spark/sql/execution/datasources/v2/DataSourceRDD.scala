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

import scala.reflect.ClassTag

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.v2.reader.ReadTask

class DataSourceRDDPartition[T : ClassTag](val index: Int, val generator: ReadTask[T])
  extends Partition with Serializable

class DataSourceRDD[T: ClassTag](
    sc: SparkContext,
    @transient private val generators: java.util.List[ReadTask[T]])
  extends RDD[T](sc, Nil) {

  override protected def getPartitions: Array[Partition] = {
    var index = 0
    val iter = generators.iterator()
    val res = new Array[Partition](generators.size())
    while (iter.hasNext) {
      res(index) = new DataSourceRDDPartition[T](index, iter.next())
      index += 1
    }
    res
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val generator = split.asInstanceOf[DataSourceRDDPartition[T]].generator
    context.addTaskCompletionListener(_ => generator.close())
    val iter = new Iterator[T] {
      generator.open()

      private[this] var valuePrepared = false

      override def hasNext: Boolean = {
        if (!valuePrepared) {
          valuePrepared = generator.next()
        }
        valuePrepared
      }

      override def next(): T = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        valuePrepared = false
        generator.get()
      }
    }
    new InterruptibleIterator(context, iter)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[DataSourceRDDPartition[T]].generator.preferredLocations()
  }
}
