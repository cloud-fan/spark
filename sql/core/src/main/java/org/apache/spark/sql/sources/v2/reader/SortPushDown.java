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
 * A mix in interface for `DataSourceV2Reader`. Users can implement this interface to pre-sort
 * the data and avoid sorting at Spark side.
 */
public interface SortPushDown {
  /**
   * Returns true if the implementation can handle this sorting requirement and save a sort
   * operation at Spark side.
   */
  boolean pushDownSort(String[] sortingColumns, boolean asc, boolean nullFirst);
}
