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

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.execution.datasources.noop.NoopWriteBuilder
import org.apache.spark.sql.sources.v2.reader.{InputPartition, ScanBuilder}
import org.apache.spark.sql.sources.v2.writer.WriteBuilder
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
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

  test("CREATE TABLE with a mismatched schema") {
    withTable("t") {
      val e = intercept[Exception](
        sql(s"CREATE TABLE t(i INT) USING ${classOf[SimpleWritableDataSource].getName}")
      )
      assert(e.getMessage.contains("please re-create table default.t"))

      sql(s"CREATE TABLE t(i LONG, j LONG) USING ${classOf[SimpleWritableDataSource].getName}")
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

  test("write-only table") {
    withTable("t") {
      sql(s"CREATE TABLE t USING ${classOf[WriteOnlyV2Source].getName}")

      val e1 = intercept[AnalysisException](sql("INSERT INTO t SELECT 1, 1"))
      assert(e1.message.contains("too many data columns"))

      sql("INSERT INTO t SELECT 1")

      val e2 = intercept[AnalysisException](sql("SELECT * FROM t").collect())
      assert(e2.message.contains("Table does not support reads"))
    }
  }

  test("CREATE TABLE AS SELECT") {
    withTable("t") {
      sql(
        s"""
           |CREATE TABLE t USING ${classOf[SimpleWritableDataSource].getName}
           |AS SELECT 1L AS i, -1L AS j
         """.stripMargin)
      checkAnswer(spark.table("t"), Row(1, -1))

      sql("INSERT INTO t SELECT 2, -2")
      checkAnswer(spark.table("t"), Seq(Row(1, -1), Row(2, -2)))
    }
  }

  test("INSERT OVERWRITE") {
    withTable("t") {
      sql(s"CREATE TABLE t USING ${classOf[SimpleWritableDataSource].getName}")

      sql("INSERT INTO t SELECT 1, -1")
      checkAnswer(spark.table("t"), Row(1, -1))

      sql("INSERT OVERWRITE t SELECT 2, -2")
      checkAnswer(spark.table("t"), Row(2, -2))
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

class WriteOnlyV2Source extends TableProvider {
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new Table with SupportsWrite {
      override def name(): String = "write-only"

      override def schema(): StructType = new StructType().add("i", "int")

      override def capabilities(): util.Set[TableCapability] = {
        Set(TableCapability.BATCH_WRITE).asJava
      }

      override def newWriteBuilder(options: CaseInsensitiveStringMap): WriteBuilder = {
        NoopWriteBuilder
      }
    }
  }
}
