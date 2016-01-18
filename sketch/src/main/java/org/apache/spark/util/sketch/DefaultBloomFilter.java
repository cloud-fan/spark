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

public class DefaultBloomFilter implements BloomFilter {

  private final int numHashFunctions;

  public DefaultBloomFilter(int numHashFunctions) {
  }

  /**
   * Computes the optimal k (number of hashes per element inserted in Bloom filter), given the
   * expected insertions and total number of bits in the Bloom filter.
   *
   * See http://en.wikipedia.org/wiki/File:Bloom_filter_fp_probability.svg for the formula.
   *
   * @param n expected insertions (must be positive)
   * @param m total number of bits in Bloom filter (must be positive)
   */
  static int optimalNumOfHashFunctions(long n, long m) {
    // (m / n) * log(2), but avoid truncation due to division!
    return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
  }

  public static DefaultBloomFilter create(int sizeInBytes) {
    return null;
  }

  public static DefaultBloomFilter create(long numItems, double fpp) {
    return null;
  }

  public static long computeSizeInBytes(long numItems, double fpp) {
    return 0;
  }

  @Override
  public double expectedFpp() {
    return 0;
  }

  @Override
  public long sizeInBytes() {
    return 0;
  }

  @Override
  public boolean put(Object item) {
    return false;
  }

  @Override
  public boolean isCompatible(BloomFilter other) {
    return false;
  }

  @Override
  public BloomFilter mergeInPlace(BloomFilter other) {
    return null;
  }

  @Override
  public boolean mightContain(Object item) {
    return false;
  }
}