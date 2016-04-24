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

package org.apache.spark.executor

import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.storage.{BlockId, BlockStatus}


/**
 * :: DeveloperApi ::
 * Metrics tracked during the execution of a task.
 *
 * This class is wrapper around a collection of internal accumulators that represent metrics
 * associated with a task. The local values of these accumulators are sent from the executor
 * to the driver when the task completes. These values are then merged into the corresponding
 * accumulator previously registered on the driver.
 *
 * The accumulator updates are also sent to the driver periodically (on executor heartbeat)
 * and when the task failed with an exception. The [[TaskMetrics]] object itself should never
 * be sent to the driver.
 */
@DeveloperApi
class TaskMetrics private[spark] () extends Serializable {
  import InternalAccumulator._

  // Each metric is internally represented as an accumulator
  private val _executorDeserializeTime = new InternalLongAccumulator(EXECUTOR_DESERIALIZE_TIME)
  private val _executorRunTime = new InternalLongAccumulator(EXECUTOR_RUN_TIME)
  private val _resultSize = new InternalLongAccumulator(RESULT_SIZE)
  private val _jvmGCTime = new InternalLongAccumulator(JVM_GC_TIME)
  private val _resultSerializationTime = new InternalLongAccumulator(RESULT_SERIALIZATION_TIME)
  private val _memoryBytesSpilled = new InternalLongAccumulator(MEMORY_BYTES_SPILLED)
  private val _diskBytesSpilled = new InternalLongAccumulator(DISK_BYTES_SPILLED)
  private val _peakExecutionMemory = new InternalLongAccumulator(PEAK_EXECUTION_MEMORY)
  private val _updatedBlockStatuses = new BlockStatusesAccumulator(UPDATED_BLOCK_STATUSES)

  /**
   * Time taken on the executor to deserialize this task.
   */
  def executorDeserializeTime: Long = _executorDeserializeTime.localValue

  /**
   * Time the executor spends actually running the task (including fetching shuffle data).
   */
  def executorRunTime: Long = _executorRunTime.localValue

  /**
   * The number of bytes this task transmitted back to the driver as the TaskResult.
   */
  def resultSize: Long = _resultSize.localValue

  /**
   * Amount of time the JVM spent in garbage collection while executing this task.
   */
  def jvmGCTime: Long = _jvmGCTime.localValue

  /**
   * Amount of time spent serializing the task result.
   */
  def resultSerializationTime: Long = _resultSerializationTime.localValue

  /**
   * The number of in-memory bytes spilled by this task.
   */
  def memoryBytesSpilled: Long = _memoryBytesSpilled.localValue

  /**
   * The number of on-disk bytes spilled by this task.
   */
  def diskBytesSpilled: Long = _diskBytesSpilled.localValue

  /**
   * Peak memory used by internal data structures created during shuffles, aggregations and
   * joins. The value of this accumulator should be approximately the sum of the peak sizes
   * across all such data structures created in this task. For SQL jobs, this only tracks all
   * unsafe operators and ExternalSort.
   */
  def peakExecutionMemory: Long = _peakExecutionMemory.localValue

  /**
   * Storage statuses of any blocks that have been updated as a result of this task.
   */
  def updatedBlockStatuses: Seq[(BlockId, BlockStatus)] = _updatedBlockStatuses.localValue

  // Setters and increment-ers
  private[spark] def setExecutorDeserializeTime(v: Long): Unit =
    _executorDeserializeTime.setValue(v)
  private[spark] def setExecutorRunTime(v: Long): Unit = _executorRunTime.setValue(v)
  private[spark] def setResultSize(v: Long): Unit = _resultSize.setValue(v)
  private[spark] def setJvmGCTime(v: Long): Unit = _jvmGCTime.setValue(v)
  private[spark] def setResultSerializationTime(v: Long): Unit =
    _resultSerializationTime.setValue(v)
  private[spark] def incMemoryBytesSpilled(v: Long): Unit = _memoryBytesSpilled.add(v)
  private[spark] def incDiskBytesSpilled(v: Long): Unit = _diskBytesSpilled.add(v)
  private[spark] def incPeakExecutionMemory(v: Long): Unit = _peakExecutionMemory.add(v)
  private[spark] def incUpdatedBlockStatuses(v: (BlockId, BlockStatus)): Unit =
    _updatedBlockStatuses.add(v)
  private[spark] def setUpdatedBlockStatuses(v: Seq[(BlockId, BlockStatus)]): Unit =
    _updatedBlockStatuses.setValue(v)

  /**
   * Metrics related to reading data from a [[org.apache.spark.rdd.HadoopRDD]] or from persisted
   * data, defined only in tasks with input.
   */
  val inputMetrics: InputMetrics = new InputMetrics()

  /**
   * Metrics related to writing data externally (e.g. to a distributed filesystem),
   * defined only in tasks with output.
   */
  val outputMetrics: OutputMetrics = new OutputMetrics()

  /**
   * Metrics related to shuffle read aggregated across all shuffle dependencies.
   * This is defined only if there are shuffle dependencies in this task.
   */
  val shuffleReadMetrics: ShuffleReadMetrics = new ShuffleReadMetrics()

  /**
   * Metrics related to shuffle write, defined only in shuffle map stages.
   */
  val shuffleWriteMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics()

  /**
   * A list of [[TempShuffleReadMetrics]], one per shuffle dependency.
   *
   * A task may have multiple shuffle readers for multiple dependencies. To avoid synchronization
   * issues from readers in different threads, in-progress tasks use a [[TempShuffleReadMetrics]]
   * for each dependency and merge these metrics before reporting them to the driver.
   */
  @transient private lazy val tempShuffleReadMetrics = new ArrayBuffer[TempShuffleReadMetrics]

  /**
   * Create a [[TempShuffleReadMetrics]] for a particular shuffle dependency.
   *
   * All usages are expected to be followed by a call to [[mergeShuffleReadMetrics]], which
   * merges the temporary values synchronously. Otherwise, all temporary data collected will
   * be lost.
   */
  private[spark] def createTempShuffleReadMetrics(): TempShuffleReadMetrics = synchronized {
    val readMetrics = new TempShuffleReadMetrics
    tempShuffleReadMetrics += readMetrics
    readMetrics
  }

  /**
   * Merge values across all temporary [[ShuffleReadMetrics]] into `_shuffleReadMetrics`.
   * This is expected to be called on executor heartbeat and at the end of a task.
   */
  private[spark] def mergeShuffleReadMetrics(): Unit = synchronized {
    if (tempShuffleReadMetrics.nonEmpty) {
      shuffleReadMetrics.setMergeValues(tempShuffleReadMetrics)
    }
  }

  // Only used for test
  private[spark] val testAccum = sys.props.get("spark.testing").map(_ =>
    new InternalLongAccumulator(TEST_ACCUM))

  @transient private[spark] lazy val internalAccums: Seq[Accumulator[_, _]] = {
    val in = inputMetrics
    val out = outputMetrics
    val sr = shuffleReadMetrics
    val sw = shuffleWriteMetrics
    Seq(_executorDeserializeTime, _executorRunTime, _resultSize, _jvmGCTime,
      _resultSerializationTime, _memoryBytesSpilled, _diskBytesSpilled, _peakExecutionMemory,
      _updatedBlockStatuses, sr._remoteBlocksFetched, sr._localBlocksFetched, sr._remoteBytesRead,
      sr._localBytesRead, sr._fetchWaitTime, sr._recordsRead, sw._bytesWritten, sw._recordsWritten,
      sw._writeTime, in._bytesRead, in._recordsRead, out._bytesWritten, out._recordsWritten) ++
      testAccum
  }

  /* ========================== *
   |        OTHER THINGS        |
   * ========================== */

  private[spark] def register(sc: SparkContext): Unit = {
    internalAccums.foreach(_.register(sc))
  }

  /**
   * External accumulators registered with this task.
   */
  @transient private lazy val externalAccums = new ArrayBuffer[Accumulator[_, _]]

  private[spark] def registerAccumulator(a: Accumulator[_, _]): Unit = {
    externalAccums += a
  }

  /**
   * Return the latest updates of accumulators in this task.
   *
   * The [[AccumulableInfo.update]] field is always defined and the [[AccumulableInfo.value]]
   * field is always empty, since this represents the partial updates recorded in this task,
   * not the aggregated value across multiple tasks.
   */
  def accumulatorUpdates(): Seq[AccumulableInfo] = {
    (internalAccums ++ externalAccums).map { a => a.toInfo(Some(a.localValue), None) }
  }
}

/**
 * Internal subclass of [[TaskMetrics]] which is used only for posting events to listeners.
 * Its purpose is to obviate the need for the driver to reconstruct the original accumulators,
 * which might have been garbage-collected. See SPARK-13407 for more details.
 *
 * Instances of this class should be considered read-only and users should not call `inc*()` or
 * `set*()` methods. While we could override the setter methods to throw
 * UnsupportedOperationException, we choose not to do so because the overrides would quickly become
 * out-of-date when new metrics are added.
 */
private[spark] class ListenerTaskMetrics(accumUpdates: Seq[AccumulableInfo]) extends TaskMetrics {

  override def accumulatorUpdates(): Seq[AccumulableInfo] = accumUpdates

  override private[spark] def registerAccumulator(a: Accumulator[_, _]): Unit = {
    throw new UnsupportedOperationException("This TaskMetrics is read-only")
  }
}

private[spark] object TaskMetrics extends Logging {

  /**
   * Create an empty task metrics that doesn't register its accumulators.
   */
  def empty: TaskMetrics = new TaskMetrics

  /**
   * Construct a [[TaskMetrics]] object from a list of accumulator updates, called on driver only.
   *
   * Executors only send accumulator updates back to the driver, not [[TaskMetrics]]. However, we
   * need the latter to post task end events to listeners, so we need to reconstruct the metrics
   * on the driver.
   *
   * This assumes the provided updates contain the initial set of accumulators representing
   * internal task level metrics.
   */
  def fromAccumulatorUpdates(accumUpdates: Seq[AccumulableInfo]): TaskMetrics = {
    val definedAccumUpdates = accumUpdates.filter(_.update.isDefined)
    val metrics = new ListenerTaskMetrics(definedAccumUpdates)
    definedAccumUpdates.filter(_.internal).foreach { accInfo =>
      metrics.internalAccums.find(_.name == accInfo.name).foreach { acc =>
        acc.asInstanceOf[Accumulator[Any, Any]].add(accInfo.update.get)
      }
    }
    metrics
  }
}
