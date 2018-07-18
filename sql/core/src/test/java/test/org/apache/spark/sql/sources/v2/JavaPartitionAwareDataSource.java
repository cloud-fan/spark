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
import java.util.Arrays;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.*;
import org.apache.spark.sql.sources.v2.reader.partitioning.ClusteredDistribution;
import org.apache.spark.sql.sources.v2.reader.partitioning.Distribution;
import org.apache.spark.sql.sources.v2.reader.partitioning.Partitioning;
import org.apache.spark.sql.types.StructType;

public class JavaPartitionAwareDataSource implements DataSourceV2, ReadSupport {

  class Reader implements DataSourceReader, SupportsReportPartitioning {
    private final StructType schema = new StructType().add("a", "int").add("b", "int");

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
          splits[0] = new SpecificInputSplit(new int[]{1, 1, 3}, new int[]{4, 4, 6});
          splits[1] = new SpecificInputSplit(new int[]{2, 4, 4}, new int[]{6, 2, 2});
          return splits;
        }
      };
    }

    @Override
    public SplitReaderProvider getReaderProvider(Metadata meta) {
      return new SpecificSplitReaderProvider();
    }

    @Override
    public Partitioning outputPartitioning(Metadata metadata) {
      return new MyPartitioning();
    }
  }

  static class MyPartitioning implements Partitioning {

    @Override
    public int numPartitions() {
      return 2;
    }

    @Override
    public boolean satisfy(Distribution distribution) {
      if (distribution instanceof ClusteredDistribution) {
        String[] clusteredCols = ((ClusteredDistribution) distribution).clusteredColumns;
        return Arrays.asList(clusteredCols).contains("a");
      }

      return false;
    }
  }

  static class SpecificInputSplit implements InputSplit {
    private int[] i;
    private int[] j;

    public SpecificInputSplit(int[] i, int[] j) {
      this.i = i;
      this.j = j;
    }
  }

  static class SpecificSplitReaderProvider implements SplitReaderProvider {
    @Override
    public InputPartitionReader<InternalRow> createRowReader(InputSplit split) {
      SpecificInputSplit s = (SpecificInputSplit) split;
      assert s.i.length == s.j.length;
      return new InputPartitionReader<InternalRow>() {
        private int[] i = s.i;
        private int[] j = s.j;
        private int current = -1;

        @Override
        public boolean next() throws IOException {
          current += 1;
          return current < i.length;
        }

        @Override
        public InternalRow get() {
          return new GenericInternalRow(new Object[] {i[current], j[current]});
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
