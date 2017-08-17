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

import java.util.List;

import org.apache.spark.sql.execution.vectorized.ColumnarBatch;

/**
 * A mix in interface for `DataSourceV2Reader`. Users can implement this interface to provide
 * columnar read ability for better performance.
 */
public interface ColumnarReadSupport {
  /**
   * Similar to `DataSourceV2Reader.createReadTasks`, but return data in columnar format.
   */
  List<ReadTask<ColumnarBatch>> createColumnarReadTasks();

  /**
   * A safety door for columnar reader. It's possible that the implementation can only support
   * columnar reads for some certain columns, users can overwrite this method to fallback to
   * normal read path under some conditions.
   */
  default boolean supportsColumnarReads() {
    return true;
  }
}
