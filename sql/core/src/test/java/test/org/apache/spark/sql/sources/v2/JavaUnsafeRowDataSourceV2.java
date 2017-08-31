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

package test.org.apache.spark.sql.sources.v2;

import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.DataSourceV2SchemaProvider;
import org.apache.spark.sql.sources.v2.reader.ReadTask;
import org.apache.spark.sql.sources.v2.reader.DataSourceV2Reader;
import org.apache.spark.sql.types.StructType;

public class JavaUnsafeRowDataSourceV2 implements DataSourceV2, DataSourceV2SchemaProvider {
  class Reader extends DataSourceV2Reader {
    private final StructType schema;

    Reader(StructType schema) {
      this.schema = schema;
    }

    @Override
    public StructType readSchema() {
      return schema;
    }

    @Override
    public List<ReadTask<Row>> createReadTasks() {
      throw new IllegalStateException();
    }

    @Override
    public List<ReadTask<UnsafeRow>> createUnsafeRowReadTasks() {
      return java.util.Arrays.asList(
        new JavaUnsafeRowReadTask(0, 5),
        new JavaUnsafeRowReadTask(5, 10));
    }
  }

  @Override
  public StructType inferSchema(CaseInsensitiveMap<String> options) {
    return new StructType().add("i", "int").add("j", "int");
  }

  @Override
  public DataSourceV2Reader createReader(StructType schema, CaseInsensitiveMap<String> options) {
    return new Reader(schema);
  }
}

class JavaUnsafeRowReadTask implements ReadTask<UnsafeRow> {
  private int start;
  private int end;
  private UnsafeRow row;

  public JavaUnsafeRowReadTask(int start, int end) {
    this.start = start - 1;
    this.end = end;
    this.row = new UnsafeRow(2);
    row.pointTo(new byte[8 * 3], 8 * 3);
  }

  @Override
  public boolean next() {
    start += 1;
    return start < end;
  }

  @Override
  public UnsafeRow get() {
    row.setInt(0, start);
    row.setInt(1, -start);
    return row;
  }
}
