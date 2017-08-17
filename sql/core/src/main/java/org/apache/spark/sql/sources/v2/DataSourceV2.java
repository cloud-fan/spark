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

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap;
import org.apache.spark.sql.sources.v2.reader.DataSourceV2Reader;
import org.apache.spark.sql.types.StructType;

/**
 * The main interface and minimal requirement for data source v2 implementations. Users can mix in
 * more interfaces to implement more functions other than just scan.
 */
public interface DataSourceV2 {

  /**
   * The main entrance for read interface.
   *
   * @param schema the full schema of this data source reader. Full schema usually maps to the
   *               physical schema of the underlying storage of this data source reader, e.g.
   *               parquet files, JDBC tables, etc, while this reader may not read data with full
   *               schema, as column pruning or other optimizations may happen.
   * @param options the options for this data source reader, which is case insensitive.
   * @return a reader that implements the actual read logic.
   */
  DataSourceV2Reader createReader(
      StructType schema,
      CaseInsensitiveMap<String> options);
}
