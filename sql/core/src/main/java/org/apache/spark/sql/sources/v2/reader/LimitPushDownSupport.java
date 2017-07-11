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
 * A mix in interface for `DataSourceV2Reader`. Users can implement this interface to push down
 * "limit" operation to the data source and reduce the size of the data to be read.
 */
public interface LimitPushDownSupport {
  /**
   * Returns true if the implementation can handle the limit operation, so that we can reduce
   * the data size to be read at the very beginning.
   */
  boolean pushDownLimit(int limit);
}
