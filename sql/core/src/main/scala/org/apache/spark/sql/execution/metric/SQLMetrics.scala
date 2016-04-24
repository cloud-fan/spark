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

package org.apache.spark.sql.execution.metric

import java.text.NumberFormat

import org.apache.spark._
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.util.Utils


private[sql] abstract class SQLMetrics extends NewAccumulator[Long, Long] {
  @transient private[this] var _sum = 0L

  override def add(v: Long): Unit = _sum += v

  override def +=(v: Long): Unit = _sum += v

  override def merge(other: NewAccumulator[Long, Long]): Unit = other match {
    case m: SQLMetrics => _sum += m.value
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: Long = _sum

  def reset(): Unit = _sum = 0L

  def metricType: String

  // Provide special identifier as metadata so we can tell that this is a `SQLMetric` later
  private[spark] override def toInfo(update: Option[Any], value: Option[Any]): AccumulableInfo = {
    assertRegistered()
    new AccumulableInfo(id, metadata.name, update, value, true, metadata.countFailedValues,
      Some(SQLMetrics.ACCUM_IDENTIFIER))
  }
}

private[sql] class SumSQLMetrics extends SQLMetrics {
  override def metricType: String = "sum"
}

private[sql] class StatisticalSQLMetrics(val metricType: String) extends SQLMetrics {
  override def initialize(): Unit = {
    // This is a workaround for SPARK-11013.
    // We use -1 as initial value of the accumulator, if the accumulator is valid, we will update
    // it at the end of task and the value will be at least 0. Then we can filter out the -1 values
    // before calculate max, min, etc.
    add(-1L)
  }
}


private[sql] object SQLMetrics {

  // Identifier for distinguishing SQL metrics from other accumulators
  private[sql] val ACCUM_IDENTIFIER = "sql"

  def createSumMetric(sc: SparkContext, name: String): SQLMetrics = {
    val acc = new SumSQLMetrics
    acc.register(sc, name = Some(name))
    acc
  }

  /**
   * Create a metric to report the size information (including total, min, med, max) like data size,
   * spill size, etc.
   */
  def createSizeMetric(sc: SparkContext, name: String): SQLMetrics = {
    // The final result of this metric in physical operator UI may looks like:
    // data size total (min, med, max):
    // 100GB (100MB, 1GB, 10GB)
    val acc = new StatisticalSQLMetrics("size")
    acc.register(sc, name = Some(s"$name total (min, med, max)"))
    acc
  }

  def createTimingMetric(sc: SparkContext, name: String): SQLMetrics = {
    // The final result of this metric in physical operator UI may looks like:
    // duration(min, med, max):
    // 5s (800ms, 1s, 2s)
    val acc = new StatisticalSQLMetrics("time")
    acc.register(sc, name = Some(s"$name total (min, med, max)"))
    acc
  }

  /**
   * A function that defines how we aggregate the final accumulator results among all tasks,
   * and represent it in string for a SQL physical operator.
   */
  def stringValue(metricsType: String, values: Seq[Long]): String = {
    if (metricsType == "sum") {
      NumberFormat.getInstance().format(values.sum)
    } else {
      val strFormat: Long => String = if (metricsType == "size") {
        Utils.bytesToString
      } else if (metricsType == "time") {
        Utils.msDurationToString
      } else {
        throw new IllegalStateException("unexpected metrics type: " + metricsType)
      }

      val validValues = values.filter(_ >= 0)
      val Seq(sum, min, med, max) = {
        val metric = if (validValues.length == 0) {
          Seq.fill(4)(0L)
        } else {
          val sorted = validValues.sorted
          Seq(sorted.sum, sorted(0), sorted(validValues.length / 2), sorted(validValues.length - 1))
        }
        metric.map(strFormat)
      }
      s"\n$sum ($min, $med, $max)"
    }
  }
}
