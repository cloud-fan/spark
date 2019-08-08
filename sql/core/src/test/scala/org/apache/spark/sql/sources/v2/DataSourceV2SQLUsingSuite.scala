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

package org.apache.spark.sql.sources.v2

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.sources.v2.reader.{InputPartition, ScanBuilder}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DataSourceV2SQLUsingSuite extends QueryTest with SharedSparkSession {

  test("basic") {
    withTable("t") {
      sql(s"CREATE TABLE t USING ${classOf[SimpleWritableDataSource].getName}")

      val e = intercept[AnalysisException](sql("INSERT INTO t SELECT 1"))
      assert(e.message.contains("not enough data columns"))

      sql("INSERT INTO t SELECT 1, -1")
      checkAnswer(spark.table("t"), Row(1, -1))
    }
  }

  test("read-only table") {
    withTable("t") {
      sql(s"CREATE TABLE t USING ${classOf[ReadOnlyV2Source].getName}")
      checkAnswer(spark.table("t"), (0 until 10).map(i => Row(i, -i)))

      val e1 = intercept[AnalysisException](sql("INSERT INTO t SELECT 1"))
      assert(e1.message.contains("not enough data columns"))

      val e2 = intercept[AnalysisException](sql("INSERT INTO t SELECT 1, -1"))
      assert(e2.message.contains("Table does not support append in batch mode"))
    }
  }
}

class ReadOnlyV2Source extends TableProvider {
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new SimpleBatchTable {
      override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
        new SimpleScanBuilder {
          override def planInputPartitions(): Array[InputPartition] = {
            Array(RangeInputPartition(0, 5), RangeInputPartition(5, 10))
          }
        }
      }
    }
  }
}
