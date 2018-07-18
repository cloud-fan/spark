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

import java.io.IOException;
import java.util.*;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.*;
import org.apache.spark.sql.types.StructType;

public class JavaAdvancedDataSourceV2 implements DataSourceV2, ReadSupport {

  public class Reader implements DataSourceReader {

    @Override
    public Metadata getMetadata() {
      return new AdvancedMetadata();
    }

    @Override
    public SplitManager getSplitManager(Metadata meta) {
      AdvancedMetadata m = (AdvancedMetadata) meta;
      return new SplitManager() {
        @Override
        public InputSplit[] getSplits() {
          List<InputSplit> res = new ArrayList<>();

          Integer lowerBound = null;
          for (Filter filter : m.filters) {
            if (filter instanceof GreaterThan) {
              GreaterThan f = (GreaterThan) filter;
              if ("i".equals(f.attribute()) && f.value() instanceof Integer) {
                lowerBound = (Integer) f.value();
                break;
              }
            }
          }

          if (lowerBound == null) {
            res.add(new JavaRangeInputSplit(0, 5));
            res.add(new JavaRangeInputSplit(5, 10));
          } else if (lowerBound < 4) {
            res.add(new JavaRangeInputSplit(lowerBound + 1, 5));
            res.add(new JavaRangeInputSplit(5, 10));
          } else if (lowerBound < 9) {
            res.add(new JavaRangeInputSplit(lowerBound + 1, 10));
          }

          return res.stream().toArray(InputSplit[]::new);
        }
      };
    }

    @Override
    public SplitReaderProvider getReaderProvider(Metadata meta) {
      return new AdvancedSplitReaderProvider(((AdvancedMetadata) meta).requiredSchema);
    }
  }

  public static class AdvancedMetadata implements Metadata,
    SupportsPushDownRequiredColumns, SupportsPushDownFilters {

    // Exposed for testing.
    public StructType requiredSchema = new StructType().add("i", "int").add("j", "int");
    public Filter[] filters = new Filter[0];

    @Override
    public StructType getSchema() {
      return requiredSchema;
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
      this.requiredSchema = requiredSchema;
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
      Filter[] supported = Arrays.stream(filters).filter(f -> {
        if (f instanceof GreaterThan) {
          GreaterThan gt = (GreaterThan) f;
          return gt.attribute().equals("i") && gt.value() instanceof Integer;
        } else {
          return false;
        }
      }).toArray(Filter[]::new);

      Filter[] unsupported = Arrays.stream(filters).filter(f -> {
        if (f instanceof GreaterThan) {
          GreaterThan gt = (GreaterThan) f;
          return !gt.attribute().equals("i") || !(gt.value() instanceof Integer);
        } else {
          return true;
        }
      }).toArray(Filter[]::new);

      this.filters = supported;
      return unsupported;
    }

    @Override
    public Filter[] pushedFilters() {
      return filters;
    }
  }

  static class AdvancedSplitReaderProvider implements SplitReaderProvider {
    StructType requiredSchema;

    public AdvancedSplitReaderProvider(StructType requiredSchema) {
      this.requiredSchema = requiredSchema;
    }

    @Override
    public InputPartitionReader<InternalRow> createRowReader(InputSplit split) {
      JavaRangeInputSplit s = (JavaRangeInputSplit) split;
      return new InputPartitionReader<InternalRow>() {
        int start = s.start - 1;
        int end = s.end;

        @Override
        public boolean next() {
          start += 1;
          return start < end;
        }

        @Override
        public InternalRow get() {
          Object[] values = new Object[requiredSchema.size()];
          for (int i = 0; i < values.length; i++) {
            if ("i".equals(requiredSchema.apply(i).name())) {
              values[i] = start;
            } else if ("j".equals(requiredSchema.apply(i).name())) {
              values[i] = -start;
            }
          }
          return new GenericInternalRow(values);
        }

        @Override
        public void close() throws IOException {

        }
      };
    }
  }

  @Override
  public DataSourceReader createReader(DataSourceOptions options) {
    return new Reader();
  }
}
