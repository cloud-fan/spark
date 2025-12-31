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

package org.apache.spark.sql.execution.matchrecognize

import scala.collection.mutable.ArrayDeque

import org.apache.spark.sql.catalyst.expressions.UnsafeRow

/**
 * A row buffer that stores UnsafeRow objects with logical offset mapping.
 *
 * The RowStore maintains:
 * - A circular buffer (implemented as ArrayDeque) of UnsafeRow copies
 * - A headOffset that maps global row indices to buffer positions
 *
 * This allows efficient:
 * - Appending new rows at the end
 * - Random access by global offset
 * - Pruning old rows from the front
 *
 * Memory management: Rows that are no longer referenced by any active branch
 * can be pruned from the front of the buffer.
 */
class RowStore {
  private val buffer = new ArrayDeque[UnsafeRow]()
  private var headOffset: Long = 0L
  private var nextOffset: Long = 0L

  /**
   * Adds a row to the store and returns its global offset.
   * The row is copied to ensure it can be safely stored.
   *
   * @param row The row to add
   * @return The global offset of the added row
   */
  def addRow(row: UnsafeRow): Long = {
    // Copy the row to ensure we have our own copy
    buffer.addOne(row.copy())
    val offset = nextOffset
    nextOffset += 1
    offset
  }

  /**
   * Retrieves a row by its global offset.
   *
   * Row store pruning ensures all rows needed by active states are available,
   * so this should never fail for valid offsets. Throws on invalid access
   * (indicating a bug in pruning logic).
   *
   * @param offset The global offset of the row
   * @return The row at the given offset
   * @throws IndexOutOfBoundsException if the offset is invalid or pruned
   */
  def getRow(offset: Long): UnsafeRow = {
    val bufferIndex = (offset - headOffset).toInt
    if (bufferIndex < 0 || bufferIndex >= buffer.size) {
      throw new IndexOutOfBoundsException(
        s"Row offset $offset is out of bounds. headOffset=$headOffset, bufferSize=${buffer.size}")
    } else {
      buffer(bufferIndex)
    }
  }

  /**
   * Retrieves multiple rows by their global offsets, in the order specified.
   *
   * @param offsets The global offsets of the rows to retrieve
   * @return The rows in the specified order
   */
  def getRows(offsets: Seq[Long]): Seq[UnsafeRow] = {
    offsets.map(getRow)
  }

  /**
   * Prunes all rows before the given offset.
   * After pruning, the headOffset is updated to the new minimum offset.
   *
   * @param newHeadOffset The new minimum offset to keep
   */
  def pruneBeforeOffset(newHeadOffset: Long): Unit = {
    if (newHeadOffset > headOffset) {
      val rowsToRemove = (newHeadOffset - headOffset).toInt.min(buffer.size)
      for (_ <- 0 until rowsToRemove) {
        buffer.removeHead()
      }
      headOffset = newHeadOffset
    }
  }

  /**
   * Returns the current head offset (minimum stored offset).
   */
  def getHeadOffset: Long = headOffset

  /**
   * Returns the next offset that will be assigned (current tail + 1).
   */
  def getNextOffset: Long = nextOffset

  /**
   * Returns the number of rows currently stored.
   */
  def size: Int = buffer.size

  /**
   * Checks if the store is empty.
   */
  def isEmpty: Boolean = buffer.isEmpty

  /**
   * Checks if a given offset is currently stored and valid.
   */
  def containsOffset(offset: Long): Boolean = {
    offset >= headOffset && offset < nextOffset
  }

  /**
   * Clears all stored rows and resets offsets.
   * Called when partition processing is complete.
   */
  def clear(): Unit = {
    buffer.clear()
    headOffset = 0L
    nextOffset = 0L
  }

  override def toString: String = {
    s"RowStore(headOffset=$headOffset, nextOffset=$nextOffset, size=${buffer.size})"
  }
}

