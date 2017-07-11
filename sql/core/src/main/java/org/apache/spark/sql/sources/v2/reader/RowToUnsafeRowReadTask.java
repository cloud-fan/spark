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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;

public class RowToUnsafeRowReadTask implements ReadTask<UnsafeRow> {
  private final ReadTask<Row> rowGenerator;
  private final StructType schema;

  private ExpressionEncoder<Row> encoder;

  public RowToUnsafeRowReadTask(ReadTask<Row> rowGenerator, StructType schema) {
    this.rowGenerator = rowGenerator;
    this.schema = schema;
  }

  @Override
  public String[] preferredLocations() {
    return rowGenerator.preferredLocations();
  }

  @Override
  public void open() {
    rowGenerator.open();
    encoder = RowEncoder.apply(schema);
  }

  @Override
  public boolean next() {
    return rowGenerator.next();
  }

  @Override
  public UnsafeRow get() {
    return (UnsafeRow) encoder.toRow(rowGenerator.get());
  }

  @Override
  public void close() {
    rowGenerator.close();
  }
}
