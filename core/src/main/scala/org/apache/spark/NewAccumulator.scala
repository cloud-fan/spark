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

package org.apache.spark

import java.{lang => jl}
import java.io.{ObjectInputStream, ObjectOutputStream}
import java.util.concurrent.atomic.AtomicLong
import javax.annotation.concurrent.GuardedBy

import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.util.Utils


private[spark] case class AccumulatorMetadata(
    id: Long,
    name: Option[String],
    countFailedValues: Boolean) extends Serializable


abstract class NewAccumulator[IN, OUT] extends Serializable {
  private[spark] var metadata: AccumulatorMetadata = _

  private[spark] def register(
      sc: SparkContext,
      id: Long = AccumulatorContext.newId(),
      name: Option[String] = None,
      countFailedValues: Boolean = false): Unit = {
    if (this.metadata != null) {
      throw new IllegalStateException("Cannot register an Accumulator twice.")
    }
    this.metadata = AccumulatorMetadata(id, name, countFailedValues)
    AccumulatorContext.register(this)
    sc.cleaner.foreach(_.registerAccumulatorForCleanup(this))
  }

  private[spark] def assertRegistered(): Unit = {
    if (metadata == null) {
      throw new IllegalStateException("Accumulator is not registered yet")
    }
  }

  def id: Long = {
    assertRegistered()
    metadata.id
  }

  def initialize(): Unit = {}

  def add(v: IN): Unit

  def +=(v: IN): Unit = add(v)

  def merge(other: NewAccumulator[IN, OUT]): Unit

  def ++=(other: NewAccumulator[IN, OUT]): Unit = merge(other)

  def value: OUT

  private[spark] def toInfo(update: Option[Any], value: Option[Any]): AccumulableInfo = {
    assertRegistered()
    val isInternal = metadata.name.exists(_.startsWith(InternalAccumulator.METRICS_PREFIX))
    new AccumulableInfo(
      metadata.id, metadata.name, update, value, isInternal, metadata.countFailedValues)
  }

  // Called by Java when serializing an object
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    assertRegistered()
    out.defaultWriteObject()
  }

  // Called by Java when deserializing an object
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    initialize()

    // Automatically register the accumulator when it is deserialized with the task closure.
    // This is for external accumulators and internal ones that do not represent task level
    // metrics, e.g. internal SQL metrics, which are per-operator.
    val taskContext = TaskContext.get()
    if (taskContext != null) {
      taskContext.registerAccumulator(this)
    }
  }
}

object AccumulatorContext {

  /**
   * This global map holds the original accumulator objects that are created on the driver.
   * It keeps weak references to these objects so that accumulators can be garbage-collected
   * once the RDDs and user-code that reference them are cleaned up.
   * TODO: Don't use a global map; these should be tied to a SparkContext (SPARK-13051).
   */
  @GuardedBy("AccumulatorContext")
  private val originals = new java.util.HashMap[Long, jl.ref.WeakReference[NewAccumulator[_, _]]]

  private[this] val nextId = new AtomicLong(0L)

  /**
   * Return a globally unique ID for a new [[NewAccumulator]].
   * Note: Once you copy the [[NewAccumulator]] the ID is no longer unique.
   */
  def newId(): Long = nextId.getAndIncrement

  /**
   * Register an [[NewAccumulator]] created on the driver such that it can be used on the executors.
   *
   * All accumulators registered here can later be used as a container for accumulating partial
   * values across multiple tasks. This is what [[org.apache.spark.scheduler.DAGScheduler]] does.
   * Note: if an accumulator is registered here, it should also be registered with the active
   * context cleaner for cleanup so as to avoid memory leaks.
   *
   * If an [[NewAccumulator]] with the same ID was already registered, this does nothing instead
   * of overwriting it. This happens when we copy accumulators, e.g. when we reconstruct
   * [[org.apache.spark.executor.TaskMetrics]] from accumulator updates.
   */
  def register(a: NewAccumulator[_, _]): Unit = synchronized {
    if (!originals.containsKey(a.id)) {
      originals.put(a.id, new jl.ref.WeakReference[NewAccumulator[_, _]](a))
    }
  }

  /**
   * Unregister the [[NewAccumulator]] with the given ID, if any.
   */
  def remove(id: Long): Unit = synchronized {
    originals.remove(id)
  }

  /**
   * Return the [[NewAccumulator]] registered with the given ID, if any.
   */
  def get(id: Long): Option[NewAccumulator[_, _]] = synchronized {
    Option(originals.get(id)).map { ref =>
      // Since we are storing weak references, we must check whether the underlying data is valid.
      val acc = ref.get
      if (acc eq null) {
        throw new IllegalAccessError(s"Attempted to access garbage collected accumulator $id")
      }
      acc
    }
  }

  /**
   * Clear all registered [[NewAccumulator]]s. For testing only.
   */
  def clear(): Unit = synchronized {
    originals.clear()
  }
}


class IntAccumulator extends NewAccumulator[jl.Integer, jl.Integer] {
  @transient private[this] var _sum = 0

  override def add(v: jl.Integer): Unit = {
    _sum += v
  }

  override def merge(other: NewAccumulator[jl.Integer, jl.Integer]): Unit = other match {
    case o: IntAccumulator => _sum += o.sum
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: jl.Integer = _sum

  def sum: Int = _sum

  def setValue(newValue: Int): Unit = _sum = newValue
}


class LongAccumulator extends NewAccumulator[jl.Long, jl.Long] {
  @transient private[this] var _sum = 0L

  override def add(v: jl.Long): Unit = {
    _sum += v
  }

  override def merge(other: NewAccumulator[jl.Long, jl.Long]): Unit = other match {
    case o: LongAccumulator => _sum += o.sum
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: jl.Long = _sum

  def sum: Long = _sum

  def setValue(newValue: Long): Unit = _sum = newValue
}


class DoubleAccumulator extends NewAccumulator[jl.Double, jl.Double] {
  @transient private[this] var _sum = 0.0

  override def add(v: jl.Double): Unit = {
    _sum += v
  }

  override def merge(other: NewAccumulator[jl.Double, jl.Double]): Unit = other match {
    case o: DoubleAccumulator => _sum += o.sum
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: jl.Double = _sum

  def sum: Double = _sum

  def setValue(newValue: Double): Unit = _sum = newValue
}


class AverageAccumulator extends NewAccumulator[jl.Double, jl.Double] {
  @transient private[this] var _sum = 0.0
  @transient private[this] var _count = 0L

  override def add(v: jl.Double): Unit = {
    _sum += v
    _count += 1
  }

  override def merge(other: NewAccumulator[jl.Double, jl.Double]): Unit = other match {
    case o: AverageAccumulator =>
      _sum += o.sum
      _count += o.count
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: jl.Double = _sum / _count

  def sum: Double = _sum

  def count: Long = _count
}


class CollectionAccumulator[T] extends NewAccumulator[T, java.util.List[T]] {
  @transient private[this] var _list: java.util.List[T] = new java.util.ArrayList[T]

  override def initialize(): Unit = {
    _list = new java.util.ArrayList[T]
  }

  override def add(v: T): Unit = {
    _list.add(v)
  }

  override def merge(other: NewAccumulator[T, java.util.List[T]]): Unit = other match {
    case o: CollectionAccumulator[T] => _list.addAll(o.value)
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: java.util.List[T] = java.util.Collections.unmodifiableList(_list)

  def setValue(newValue: java.util.List[T]): Unit = _list = newValue
}


class LegacyAccumulatorWrapper[R, T](
    initialValue: R,
    param: org.apache.spark.AccumulableParam[R, T]) extends NewAccumulator[T, R] {
  @transient private var _value = initialValue  // Current value on driver
  val zero = param.zero(initialValue) // Zero value to be passed to executors

  override def initialize(): Unit = {
    _value = zero
  }

  override def add(v: T): Unit = {
    _value = param.addAccumulator(_value, v)
  }

  override def merge(other: NewAccumulator[T, R]): Unit = {
    _value = param.addInPlace(_value, other.value)
  }

  override def value: R = _value
}
