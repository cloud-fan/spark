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
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.DataSourceV2SchemaProvider;
import org.apache.spark.sql.sources.v2.reader.ColumnarReadSupport;
import org.apache.spark.sql.sources.v2.reader.ReadTask;
import org.apache.spark.sql.sources.v2.reader.DataSourceV2Reader;
import org.apache.spark.sql.types.StructType;

public class JavaBatchDataSourceV2 implements DataSourceV2, DataSourceV2SchemaProvider {
  class Reader extends DataSourceV2Reader implements ColumnarReadSupport {
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
      throw new RuntimeException("not implemented");
    }

    @Override
    public List<ReadTask<ColumnarBatch>> createColumnarReadTasks() {
      return java.util.Arrays.asList(new JavaColumnarReadTask(schema));
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

class JavaColumnarReadTask implements ReadTask<ColumnarBatch> {
  private final StructType schema;

  public JavaColumnarReadTask(StructType schema) {
    this.schema = schema;
  }

  private ColumnarBatch batch = null;
  private int currentBatch = 0;

  @Override
  public boolean next() {
    currentBatch += 1;
    return currentBatch <= 2;
  }

  @Override
  public ColumnarBatch get() {
    if (batch == null) {
      batch = ColumnarBatch.allocate(schema);
    }
    batch.reset();
    if (currentBatch == 1) {
      for (int i = 0; i < 5; i++) {
        batch.column(0).putInt(i, i);
        batch.column(1).putInt(i, -i);
      }
    } else {
      for (int i = 0; i < 5; i++) {
        batch.column(0).putInt(i, i + 5);
        batch.column(1).putInt(i, -(i + 5));
      }
    }
    batch.setNumRows(5);
    return batch;
  }
}
