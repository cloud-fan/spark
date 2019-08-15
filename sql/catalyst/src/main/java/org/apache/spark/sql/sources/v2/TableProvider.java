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

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalog.v2.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * The base interface for v2 data sources which don't have a real catalog. Implementations must
 * have a public, 0-arg constructor.
 * <p>
 * Note that, TableProvider can only apply data operations to existing tables, like read, append,
 * delete, and overwrite. It does not support the operations that require metadata changes, like
 * create/drop tables.
 * <p>
 * The major responsibility of this interface is to return a {@link Table} for read/write.
 * </p>
 */
@Evolving
public interface TableProvider {

  /**
   * Return a {@link Table} instance to do read/write with user-specified options.
   *
   * @param options the user-specified options that can identify a table, e.g. file path, Kafka
   *                topic name, etc. It's an immutable case-insensitive string-to-string map.
   */
  Table getTable(CaseInsensitiveStringMap options);

  /**
   * Return a {@link Table} instance to do read/write with user-specified options and additional
   * schema/partitions information. The additional schema/partitions information can be specified
   * by users (e.g. {@code session.read.format("myFormat").schema(...)}) or retrieved from the
   * metastore (e.g. {@code CREATE TABLE t(i INT) USING myFormat}).
   * <p>
   * The returned table must report the same schema/partitions with the ones that are passed in.
   * <p>
   * By default this method throws {@link UnsupportedOperationException}, implementations should
   * override this method to handle the additional schema/partitions information.
   * </p>
   * @param options the user-specified options that can identify a table, e.g. file path, Kafka
   *                topic name, etc. It's an immutable case-insensitive string-to-string map.
   * @param schema the additional schema information.
   * @param partitions the additional partitions information.
   * @throws UnsupportedOperationException
   */
  default Table getTable(
      CaseInsensitiveStringMap options,
      StructType schema,
      Transform[] partitions) {
    throw new UnsupportedOperationException(
      this.getClass().getSimpleName() +
        " source does not support additional schema/partitions information.");
  }
}
