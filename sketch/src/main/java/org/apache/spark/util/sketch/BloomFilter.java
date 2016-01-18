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

package org.apache.spark.util.sketch;

public interface BloomFilter {
  /**
   * Returns the probability that {@linkplain #mightContain(Object)} will erroneously return
   * {@code true} for an object that has not actually been put in the {@code BloomFilter}.
   *
   * <p>Ideally, this number should be close to the {@code fpp} parameter
   * passed in to create this bloom filter, or smaller. If it is
   * significantly higher, it is usually the case that too many elements (more than
   * expected) have been put in the {@code BloomFilter}, degenerating it.
   */
  double expectedFpp();

  /**
   * Returns number of bytes this bloom filter used to build the underlying bit set.
   */
  long sizeInBytes();

  /**
   * Puts an element into this {@code BloomFilter}. Ensures that subsequent invocations of
   * {@link #mightContain(Object)} with the same element will always return {@code true}.
   *
   * @return true if the bloom filter's bits changed as a result of this operation. If the bits
   *     changed, this is <i>definitely</i> the first time {@code object} has been added to the
   *     filter. If the bits haven't changed, this <i>might</i> be the first time {@code object}
   *     has been added to the filter. Note that {@code put(t)} always returns the
   *     <i>opposite</i> result to what {@code mightContain(t)} would have returned at the time
   *     it is called."
   */
  boolean put(Object item);

  /**
   * Determines whether a given bloom filter is compatible with this bloom filter. For two
   * bloom filters to be compatible, they must have the same bit size.
   *
   * @param other The bloom filter to check for compatibility.
   */
  boolean isCompatible(BloomFilter other);

  /**
   * Combines this bloom filter with another bloom filter by performing a bitwise OR of the
   * underlying data. The mutations happen to <b>this</b> instance. Callers must ensure the
   * bloom filters are appropriately sized to avoid saturating them.
   *
   * @param other The bloom filter to combine this bloom filter with. It is not mutated.
   * @throws IllegalArgumentException if {@code isCompatible(that) == false}
   */
  BloomFilter mergeInPlace(BloomFilter other);

  /**
   * Returns {@code true} if the element <i>might</i> have been put in this Bloom filter,
   * {@code false} if this is <i>definitely</i> not the case.
   */
  boolean mightContain(Object item);
}
