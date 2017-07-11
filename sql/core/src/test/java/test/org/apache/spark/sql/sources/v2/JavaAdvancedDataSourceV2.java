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

import java.util.*;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.DataSourceV2SchemaProvider;
import org.apache.spark.sql.sources.v2.reader.ColumnPruningSupport;
import org.apache.spark.sql.sources.v2.reader.ReadTask;
import org.apache.spark.sql.sources.v2.reader.DataSourceV2Reader;
import org.apache.spark.sql.sources.v2.reader.FilterPushDownSupport;
import org.apache.spark.sql.types.StructType;

public class JavaAdvancedDataSourceV2 implements DataSourceV2, DataSourceV2SchemaProvider {
  class Reader extends DataSourceV2Reader implements ColumnPruningSupport, FilterPushDownSupport {
    private StructType requiredSchema;
    private List<Filter> filters = new LinkedList<>();

    Reader(StructType schema) {
      this.requiredSchema = schema;
    }

    @Override
    public StructType readSchema() {
      return requiredSchema;
    }

    @Override
    public boolean pruneColumns(StructType requiredSchema) {
      this.requiredSchema = requiredSchema;
      return true;
    }

    @Override
    public boolean pushDownFilter(Filter filter) {
      this.filters.add(filter);
      return true;
    }

    @Override
    public List<ReadTask<Row>> createReadTasks() {
      List<ReadTask<Row>> res = new ArrayList<>();

      Integer lowerBound = null;
      for (Filter filter : filters) {
        if (filter instanceof GreaterThan) {
          GreaterThan f = (GreaterThan) filter;
          if ("i".equals(f.attribute()) && f.value() instanceof Integer) {
            lowerBound = (Integer) f.value();
            break;
          }
        }
      }

      if (lowerBound == null) {
        res.add(new JavaAdvancedReadTask(0, 5, requiredSchema));
        res.add(new JavaAdvancedReadTask(5, 10, requiredSchema));
      } else if (lowerBound < 4) {
        res.add(new JavaAdvancedReadTask(lowerBound + 1, 5, requiredSchema));
        res.add(new JavaAdvancedReadTask(5, 10, requiredSchema));
      } else if (lowerBound < 9) {
        res.add(new JavaAdvancedReadTask(lowerBound + 1, 10, requiredSchema));
      }

      return res;
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

class JavaAdvancedReadTask implements ReadTask<Row> {
  private int start;
  private int end;
  private StructType requiredSchema;

  public JavaAdvancedReadTask(int start, int end, StructType requiredSchema) {
    this.start = start - 1;
    this.end = end;
    this.requiredSchema = requiredSchema;
  }

  @Override
  public boolean next() {
    start += 1;
    return start < end;
  }

  @Override
  public Row get() {
    Object[] values = new Object[requiredSchema.size()];
    for (int i = 0; i < values.length; i++) {
      if ("i".equals(requiredSchema.apply(i).name())) {
        values[i] = start;
      } else if ("j".equals(requiredSchema.apply(i).name())) {
        values[i] = -start;
      }
    }
    return new GenericRow(values);
  }
}
