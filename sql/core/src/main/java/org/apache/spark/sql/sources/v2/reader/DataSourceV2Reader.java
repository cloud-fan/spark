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

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;

/**
 * The main interface and minimal requirement for a data source reader. The implementations should
 * at least implement the full scan logic, users can mix in more interfaces to implement scan
 * optimizations like column pruning, filter push down, etc.
 */
public abstract class DataSourceV2Reader {

  /**
   * The actual schema of this data source reader, which may be different from the physical schema
   * of the underlying storage, as column pruning or other optimizations may happen.
   */
  public abstract StructType readSchema();

  /**
   * The actual read logic should be implemented here. This may not be a full scan as optimizations
   * may have already been applied on this reader. Implementations should return a list of
   * read tasks, each task is responsible to output data for one RDD partition, which means
   * the number of tasks returned here will be same as the number of RDD partitions this scan
   * output.
   */
  // TODO: maybe we should support arbitrary type and work with Dataset, instead of only Row.
  protected abstract List<ReadTask<Row>> createReadTasks();

  /**
   * Inside Spark, the input rows will be converted to `UnsafeRow`s before processing. To avoid
   * this conversion, implementations can overwrite this method and output `UnsafeRow`s directly.
   * Note that, this is an experimental and unstable interface, as `UnsafeRow` is not public and
   * may get changed in future Spark versions.
   *
   * Note that, if the implement overwrites this method, he should also overwrite `createReadTasks`
   * to throw exception, as it will never be called.
   */
  @Experimental
  @InterfaceStability.Unstable
  public List<ReadTask<UnsafeRow>> createUnsafeRowReadTasks() {
    StructType schema = readSchema();
    return createReadTasks().stream()
        .map(rowGenerator -> new RowToUnsafeRowReadTask(rowGenerator, schema))
        .collect(Collectors.toList());
  }
}

class RowToUnsafeRowReadTask implements ReadTask<UnsafeRow> {
  private final ReadTask<Row> rowReadTask;
  private final StructType schema;

  RowToUnsafeRowReadTask(ReadTask<Row> rowReadTask, StructType schema) {
    this.rowReadTask = rowReadTask;
    this.schema = schema;
  }

  @Override
  public String[] preferredLocations() {
    return rowReadTask.preferredLocations();
  }

  @Override
  public DataReader<UnsafeRow> getReader() {
    return new RowToUnsafeDataReader(rowReadTask.getReader(), RowEncoder.apply(schema));
  }
}

class RowToUnsafeDataReader implements DataReader<UnsafeRow> {
  private final DataReader<Row> rowReader;
  private final ExpressionEncoder<Row> encoder;

  RowToUnsafeDataReader(DataReader<Row> rowReader, ExpressionEncoder<Row> encoder) {
    this.rowReader = rowReader;
    this.encoder = encoder;
  }

  @Override
  public boolean hasNext() {
    return rowReader.hasNext();
  }

  @Override
  public UnsafeRow next() {
    return (UnsafeRow) encoder.toRow(rowReader.next());
  }

  @Override
  public void close() throws IOException {
    rowReader.close();
  }
}
