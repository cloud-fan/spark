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

package org.apache.spark.sql.sources.v2.reader.distribution;

/**
 * A mix in interface for `DataSourceV2Reader`. Users can implement this interface to report the
 * output partitioning, to avoid shuffle at Spark side if the output partitioning can satisfy the
 * distribution requirement.
 */
public interface DistributionSupport {
  /**
   * Returns an array of partitionings this data source can output. Spark will pick one partitioning
   * that can avoid shuffle, and call `pickPartitioning` to notify the data source which
   * partitioning was picked. Note that, if none of the partitions can help to avoid shuffle,
   * `NoPartitioning` will be passed to `pickPartitioning`.
   */
  Partitioning[] getPartitionings();

  void pickPartitioning(Partitioning p);
}
