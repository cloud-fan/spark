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

package org.apache.spark.sql.sources.v2;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap;
import org.apache.spark.sql.sources.v2.writer.DataSourceV2Writer;

/**
 * A mix in interface for `DataSourceV2`. Users can implement this interface to provide data writing
 * ability with job-level transaction.
 */
public interface WritableDataSourceV2 extends DataSourceV2 {

  /**
   * The main entrance for write interface.
   *
   * @param mode the save move, can be append, overwrite, etc.
   * @param options the options for this data source writer.
   * @return a writer that implements the actual write logic.
   */
  DataSourceV2Writer createWriter(SaveMode mode, CaseInsensitiveMap<String> options);
}
