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

/**
 * An interface to describe how the data is partitioned. This is returned by
 * `ClusteringPushDownSupport.pushDownClustering`, and data source implementations must guarantee
 * that the returned partitioning satisfies the clustering requirement.
 */
public interface Partitioning {
  /**
   * Returns true if this partitioning is compatible with the other partitioning. Think about
   * joining 2 data sources, even if both these 2 data sources satisfy the clustering requirement,
   * we still can not join them because they are uncompatible, e.g. different number of partitions,
   * different partitioner, etc.
   */
  boolean compatibleWith(Partitioning other);

  /**
   * The number of read tasks that will be used to produce data.
   */
  int numPartitions();
}
