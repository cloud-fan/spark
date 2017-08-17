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

package org.apache.spark.sql.sources.v2.reader;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.annotation.InterfaceStability;

/**
 * A mix in interface for `DataSourceV2Reader`. Users can implement this interface to pre-partition
 * the data and avoid shuffle at Spark side.
 *
 * Note that this interface is marked as unstable, as the implementation needs to be consistent
 * with the Spark SQL shuffle hash function, which is internal and may get changed over different
 * Spark versions.
 */
@Experimental
@InterfaceStability.Unstable
public interface HashPartitionPushDownSupport {
  /**
   * Returns true if the implementation can handle this hash partitioning requirement and save a
   * shuffle at Spark side. The hash function is defined as: constructing a
   * {@link org.apache.spark.sql.catalyst.expressions.UnsafeRow} with the values of the given
   * partition columns, and call its `hashCode()` method.
   */
  boolean pushDownHashPartition(String[] partitionColumns);
}
