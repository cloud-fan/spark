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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.*;
import org.apache.spark.sql.types.StructType;

public class JavaSimpleDataSourceV2 implements DataSourceV2, ReadSupport {

  class Reader implements DataSourceReader {
    private final StructType schema = new StructType().add("i", "int").add("j", "int");

    @Override
    public Metadata getMetadata() {
      return new SchemaOnlyMetadata(schema);
    }

    @Override
    public SplitManager getSplitManager(Metadata meta) {
      return new SplitManager() {
        @Override
        public InputSplit[] getSplits() {
          InputSplit[] splits = new InputSplit[2];
          splits[0] = new JavaRangeInputSplit(0, 5);
          splits[1] = new JavaRangeInputSplit(5, 10);
          return splits;
        }
      };
    }

    @Override
    public SplitReaderProvider getReaderProvider(Metadata meta) {
      return new SimpleSplitReaderProvider();
    }
  }

  static class SimpleSplitReaderProvider implements SplitReaderProvider {
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
          return new GenericInternalRow(new Object[] {start, -start});
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
