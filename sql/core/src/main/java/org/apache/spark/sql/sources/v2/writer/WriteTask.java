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

package org.apache.spark.sql.sources.v2.writer;

import java.io.Serializable;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;

/**
 * A write task that is responsible for writing the data for each input partition.
 */
public abstract class WriteTask implements Serializable {
  private ExpressionEncoder<Row> encoder;

  /**
   * This method will be called before writing data, users can overwrite this method and put
   * initialization logic here. Note that, when overwriting this method, you have to call
   * `super.initialize` to correctly do some internal setup for this write task.
   */
  public void initialize(StructType schema) {
    encoder = RowEncoder.apply(schema);
  }

  /**
   * The actual write logic should be implemented here. To correctly implement transaction,
   * implementations should stage the writing or have a way to rollback.
   */
  public abstract void write(Row row);

  /**
   * Inside Spark, the data is `UnsafeRow` and will be converted to `Row` before sending it out of
   * Spark and writing to data source. To avoid this conversion, implementations can overwrite
   * this method and writes `UnsafeRow`s directly. Note that, this is an experimental and unstable
   * interface, as `UnsafeRow` is not public and may get changed in future Spark versions.
   */
  @Experimental
  @InterfaceStability.Unstable
  public void write(UnsafeRow row) {
    write(encoder.fromRow(row));
  }

  public abstract WriterCommitMessage commit();

  public abstract void abort();
}
