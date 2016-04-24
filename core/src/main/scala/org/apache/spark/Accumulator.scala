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
import java.io.{ObjectInputStream, ObjectOutputStream, Serializable}
import java.util.concurrent.atomic.AtomicLong
import javax.annotation.concurrent.GuardedBy

import scala.collection.generic.Growable
import scala.reflect.ClassTag

import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.Utils


abstract class Accumulator[IN, OUT](
    val name: Option[String],
    private[spark] val countFailedValues: Boolean) extends Serializable {
  private[spark] val id = AccumulatorContext.newId()
  private[this] var atDriverSide = true

  private[spark] def register(sc: SparkContext): Unit = {
    if (isRegistered) {
      throw new UnsupportedOperationException("Cannot register an Accumulator twice.")
    }
    AccumulatorContext.register(this)
    sc.cleaner.foreach(_.registerAccumulatorForCleanup(this))
  }

  final def isRegistered: Boolean = AccumulatorContext.originals.containsKey(id)

  def initialize(): Unit = {}

  def add(v: IN): Unit

  def +=(v: IN): Unit = add(v)

  def merge(other: OUT): Unit

  def ++=(other: OUT): Unit = merge(other)

  final def value: OUT = {
    if (atDriverSide) {
      localValue
    } else {
      throw new UnsupportedOperationException("Can't read accumulator value in task")
    }
  }

  def localValue: OUT

  def isZero(v: OUT): Boolean

  private[spark] def toInfo(update: Option[Any], value: Option[Any]): AccumulableInfo = {
    val isInternal = name.exists(_.startsWith(InternalAccumulator.METRICS_PREFIX))
    new AccumulableInfo(id, name, update, value, isInternal, countFailedValues)
  }

  // Called by Java when serializing an object
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    if (!isRegistered) {
      throw new IllegalStateException(
        "Accumulator must be registered before serialize and send to executor")
    }
    out.defaultWriteObject()
  }

  // Called by Java when deserializing an object
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    initialize()
    atDriverSide = false

    // Automatically register the accumulator when it is deserialized with the task closure.
    // This is for external accumulators and internal ones that do not represent task level
    // metrics, e.g. internal SQL metrics, which are per-operator.
    val taskContext = TaskContext.get()
    if (taskContext != null) {
      taskContext.registerAccumulator(this)
    }
  }
}

private[spark] object AccumulatorContext {

  /**
   * This global map holds the original accumulator objects that are created on the driver.
   * It keeps weak references to these objects so that accumulators can be garbage-collected
   * once the RDDs and user-code that reference them are cleaned up.
   * TODO: Don't use a global map; these should be tied to a SparkContext (SPARK-13051).
   */
  @GuardedBy("AccumulatorContext")
  val originals = new java.util.HashMap[Long, jl.ref.WeakReference[Accumulator[_, _]]]

  private[this] val nextId = new AtomicLong(0L)

  /**
   * Return a globally unique ID for a new [[Accumulator]].
   * Note: Once you copy the [[Accumulator]] the ID is no longer unique.
   */
  def newId(): Long = nextId.getAndIncrement

  /**
   * Register an [[Accumulator]] created on the driver such that it can be used on the executors.
   *
   * All accumulators registered here can later be used as a container for accumulating partial
   * values across multiple tasks. This is what [[org.apache.spark.scheduler.DAGScheduler]] does.
   * Note: if an accumulator is registered here, it should also be registered with the active
   * context cleaner for cleanup so as to avoid memory leaks.
   *
   * If an [[Accumulator]] with the same ID was already registered, this does nothing instead
   * of overwriting it. This happens when we copy accumulators, e.g. when we reconstruct
   * [[org.apache.spark.executor.TaskMetrics]] from accumulator updates.
   */
  def register(a: Accumulator[_, _]): Unit = synchronized {
    if (!originals.containsKey(a.id)) {
      originals.put(a.id, new jl.ref.WeakReference[Accumulator[_, _]](a))
    }
  }

  /**
   * Unregister the [[Accumulator]] with the given ID, if any.
   */
  def remove(id: Long): Unit = synchronized {
    originals.remove(id)
  }

  /**
   * Return the [[Accumulator]] registered with the given ID, if any.
   */
  def get(id: Long): Option[Accumulator[_, _]] = synchronized {
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
   * Clear all registered [[Accumulator]]s. For testing only.
   */
  def clear(): Unit = synchronized {
    originals.clear()
  }
}


class IntAccumulator(name: Option[String] = None, countFailedValues: Boolean = false)
  extends Accumulator[jl.Integer, jl.Integer](name, countFailedValues) {
  @transient private[this] var _sum = 0

  override def isZero(v: jl.Integer): Boolean = v == 0

  override def add(v: jl.Integer): Unit = _sum += v

  override def merge(v: jl.Integer): Unit = _sum += v

  override def localValue: jl.Integer = _sum

  def sum: Int = _sum
}


class LongAccumulator(name: Option[String] = None, countFailedValues: Boolean = false)
  extends Accumulator[jl.Long, jl.Long](name, countFailedValues) {
  @transient private[this] var _sum = 0L

  override def isZero(v: jl.Long): Boolean = v == 0L

  override def add(v: jl.Long): Unit = _sum += v

  override def merge(v: jl.Long): Unit = _sum += v

  override def localValue: jl.Long = _sum

  def sum: Long = _sum
}


class DoubleAccumulator(name: Option[String] = None, countFailedValues: Boolean = false)
  extends Accumulator[jl.Double, jl.Double](name, countFailedValues) {
  @transient private[this] var _sum = 0.0

  override def isZero(v: jl.Double): Boolean = v == 0.0

  override def add(v: jl.Double): Unit = _sum += v

  override def merge(v: jl.Double): Unit = _sum += v

  override def localValue: jl.Double = _sum

  def sum: Double = _sum
}


class CollectionAccumulator[T](name: Option[String] = None, countFailedValues: Boolean = false)
  extends Accumulator[T, java.util.List[T]](name, countFailedValues) {
  @transient private[this] var _list: java.util.List[T] = new java.util.ArrayList[T]

  override def isZero(v: java.util.List[T]): Boolean = v.isEmpty

  override def initialize(): Unit = _list = new java.util.ArrayList[T]

  override def add(v: T): Unit = _list.add(v)

  override def merge(other: java.util.List[T]): Unit = _list.addAll(other)

  override def localValue: java.util.List[T] = java.util.Collections.unmodifiableList(_list)
}


/* ------------------------------------------------------------------------------------- *
 | Legacy accumulable related classes.                                                   |
 * ------------------------------------------------------------------------------------- */


/**
 * Helper object defining how to accumulate values of a particular type. An implicit
 * AccumulableParam needs to be available when you create [[Accumulable]]s of a specific type.
 *
 * @tparam R the full accumulated data (result type)
 * @tparam T partial data that can be added in
 */
trait AccumulableParam[R, T] extends Serializable {
  /**
   * Add additional data to the accumulator value. Is allowed to modify and return `r`
   * for efficiency (to avoid allocating objects).
   *
   * @param r the current value of the accumulator
   * @param t the data to be added to the accumulator
   * @return the new value of the accumulator
   */
  def addAccumulator(r: R, t: T): R

  /**
   * Merge two accumulated values together. Is allowed to modify and return the first value
   * for efficiency (to avoid allocating objects).
   *
   * @param r1 one set of accumulated data
   * @param r2 another set of accumulated data
   * @return both data sets merged together
   */
  def addInPlace(r1: R, r2: R): R

  /**
   * Return the "zero" (identity) value for an accumulator type, given its initial value. For
   * example, if R was a vector of N dimensions, this would return a vector of N zeroes.
   */
  def zero(initialValue: R): R
}


private[spark] class
GrowableAccumulableParam[R <% Growable[T] with TraversableOnce[T] with Serializable: ClassTag, T]
  extends AccumulableParam[R, T] {

  def addAccumulator(growable: R, elem: T): R = {
    growable += elem
    growable
  }

  def addInPlace(t1: R, t2: R): R = {
    t1 ++= t2
    t1
  }

  def zero(initialValue: R): R = {
    // We need to clone initialValue, but it's hard to specify that R should also be Cloneable.
    // Instead we'll serialize it to a buffer and load it back.
    val ser = new JavaSerializer(new SparkConf(false)).newInstance()
    val copy = ser.deserialize[R](ser.serialize(initialValue))
    copy.clear()   // In case it contained stuff
    copy
  }
}


/**
 * A simpler version of [[org.apache.spark.AccumulableParam]] where the only data type you can add
 * in is the same type as the accumulated value. An implicit AccumulatorParam object needs to be
 * available when you create Accumulators of a specific type.
 *
 * @tparam T type of value to accumulate
 */
trait AccumulatorParam[T] extends AccumulableParam[T, T] {
  def addAccumulator(t1: T, t2: T): T = {
    addInPlace(t1, t2)
  }
}


object AccumulatorParam {

  // The following implicit objects were in SparkContext before 1.2 and users had to
  // `import SparkContext._` to enable them. Now we move them here to make the compiler find
  // them automatically. However, as there are duplicate codes in SparkContext for backward
  // compatibility, please update them accordingly if you modify the following implicit objects.

  implicit object DoubleAccumulatorParam extends AccumulatorParam[Double] {
    def addInPlace(t1: Double, t2: Double): Double = t1 + t2
    def zero(initialValue: Double): Double = 0.0
  }

  implicit object IntAccumulatorParam extends AccumulatorParam[Int] {
    def addInPlace(t1: Int, t2: Int): Int = t1 + t2
    def zero(initialValue: Int): Int = 0
  }

  implicit object LongAccumulatorParam extends AccumulatorParam[Long] {
    def addInPlace(t1: Long, t2: Long): Long = t1 + t2
    def zero(initialValue: Long): Long = 0L
  }

  implicit object FloatAccumulatorParam extends AccumulatorParam[Float] {
    def addInPlace(t1: Float, t2: Float): Float = t1 + t2
    def zero(initialValue: Float): Float = 0f
  }
}


class LegacyAccumulatorWrapper[R, T](
    initialValue: R,
    param: org.apache.spark.AccumulableParam[R, T],
    name: Option[String] = None)
  extends Accumulator[T, R](name, false) {

  @transient private var _value = initialValue  // Current value on driver
  val zero = param.zero(initialValue) // Zero value to be passed to executors

  override def isZero(v: R): Boolean = v == zero

  override def initialize(): Unit = _value = zero

  override def add(v: T): Unit = _value = param.addAccumulator(_value, v)

  override def merge(other: R): Unit = _value = param.addInPlace(_value, other)

  override def localValue: R = _value
}
