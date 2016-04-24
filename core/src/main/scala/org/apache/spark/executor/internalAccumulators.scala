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

import org.apache.spark.Accumulator
import org.apache.spark.storage.{BlockId, BlockStatus}

import scala.collection.mutable.ArrayBuffer

class InternalIntAccumulator(name: String) extends Accumulator[Int, Int](Some(name), true) {
  @transient private[this] var _sum = 0

  override def add(v: Int): Unit = _sum += v

  override def merge(v: Int): Unit = _sum += v

  override def isZero(v: Int): Boolean = _sum == 0

  override def localValue: Int = _sum

  def setValue(newValue: Int): Unit = _sum = newValue
}

class InternalLongAccumulator(name: String) extends Accumulator[Long, Long](Some(name), true) {
  @transient private[this] var _sum = 0L

  override def add(v: Long): Unit = _sum += v

  override def merge(v: Long): Unit = _sum += v

  override def isZero(v: Long): Boolean = _sum == 0

  override def localValue: Long = _sum

  def setValue(newValue: Long): Unit = _sum = newValue
}

class BlockStatusesAccumulator(name: String)
  extends Accumulator[(BlockId, BlockStatus), Seq[(BlockId, BlockStatus)]](Some(name), true) {
  @transient private[this] var _seq = ArrayBuffer.empty[(BlockId, BlockStatus)]

  override def isZero(v: Seq[(BlockId, BlockStatus)]): Boolean = v.isEmpty

  override def initialize(): Unit = {
    _seq = ArrayBuffer.empty[(BlockId, BlockStatus)]
  }

  override def add(v: (BlockId, BlockStatus)): Unit = _seq += v

  override def merge(other: Seq[(BlockId, BlockStatus)]): Unit = _seq ++= other

  override def localValue: Seq[(BlockId, BlockStatus)] = _seq

  def setValue(newValue: Seq[(BlockId, BlockStatus)]): Unit = {
    _seq.clear()
    _seq ++= newValue
  }
}
