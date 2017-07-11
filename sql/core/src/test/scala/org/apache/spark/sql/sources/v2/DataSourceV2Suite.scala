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

import java.util.{ArrayList, List => JList}

import scala.collection.mutable.ListBuffer

import test.org.apache.spark.sql.sources.v2._

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.vectorized.ColumnarBatch
import org.apache.spark.sql.sources.{Filter, GreaterThan}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructType

class DataSourceV2Suite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("simplest implementation") {
    Seq(classOf[SimpleDataSourceV2], classOf[JavaSimpleDataSourceV2]).foreach { cls =>
      withClue(cls.getName) {
        val df = spark.read.format(cls.getName).load()
        checkAnswer(df, (0 until 10).map(i => Row(i, -i)))
        checkAnswer(df.select('j), (0 until 10).map(i => Row(-i)))
        checkAnswer(df.filter('i > 5), (6 until 10).map(i => Row(i, -i)))
      }
    }
  }

  test("advanced implementation") {
    Seq(classOf[AdvancedDataSourceV2], classOf[JavaAdvancedDataSourceV2]).foreach { cls =>
      withClue(cls.getName) {
        val df = spark.read.format(cls.getName).load()
        checkAnswer(df, (0 until 10).map(i => Row(i, -i)))
        checkAnswer(df.select('j), (0 until 10).map(i => Row(-i)))
        checkAnswer(df.filter('i > 3), (4 until 10).map(i => Row(i, -i)))
        checkAnswer(df.select('j).filter('i > 6), (7 until 10).map(i => Row(-i)))
        checkAnswer(df.select('i).filter('i > 10), Nil)
      }
    }
  }

  test("unsafe row implementation") {
    Seq(classOf[UnsafeRowDataSourceV2], classOf[JavaUnsafeRowDataSourceV2]).foreach { cls =>
      withClue(cls.getName) {
        val df = spark.read.format(cls.getName).load()
        checkAnswer(df, (0 until 10).map(i => Row(i, -i)))
        checkAnswer(df.select('j), (0 until 10).map(i => Row(-i)))
        checkAnswer(df.filter('i > 5), (6 until 10).map(i => Row(i, -i)))
      }
    }
  }

  test("batched implementation") {
    Seq(classOf[BatchDataSourceV2], classOf[JavaBatchDataSourceV2]).foreach { cls =>
      withClue(cls.getName) {
        val df = spark.read.format(cls.getName).load()
        checkAnswer(df, (0 until 10).map(i => Row(i, -i)))
        checkAnswer(df.select('j), (0 until 10).map(i => Row(-i)))
        checkAnswer(df.filter('i > 5), (6 until 10).map(i => Row(i, -i)))
      }
    }
  }
}

class SimpleDataSourceV2 extends DataSourceV2 with DataSourceV2SchemaProvider {
  class Reader(val readSchema: StructType) extends DataSourceV2Reader {
    override def createReadTasks(): JList[ReadTask[Row]] = {
      java.util.Arrays.asList(new SimpleReadTask(0, 5), new SimpleReadTask(5, 10))
    }
  }

  override def inferSchema(options: CaseInsensitiveMap[String]): StructType = {
    new StructType().add("i", "int").add("j", "int")
  }

  override def createReader(
      schema: StructType,
      options: CaseInsensitiveMap[String]): DataSourceV2Reader = new Reader(schema)
}

class SimpleReadTask(start: Int, end: Int) extends ReadTask[Row] {
  private var current = start - 1
  override def next(): Boolean = {
    current += 1
    current < end
  }
  override def get(): Row = Row(current, -current)
}



class AdvancedDataSourceV2 extends DataSourceV2 with DataSourceV2SchemaProvider {
  class Reader(fullSchema: StructType) extends DataSourceV2Reader
    with ColumnPruningSupport with FilterPushDownSupport {

    var requiredSchema = fullSchema
    val filters = ListBuffer.empty[Filter]

    override def pruneColumns(requiredSchema: StructType): Boolean = {
      this.requiredSchema = requiredSchema
      true
    }

    override def pushDownFilter(filter: Filter): Boolean = {
      this.filters += filter
      true
    }

    override def readSchema(): StructType = {
      requiredSchema
    }

    override def createReadTasks(): JList[ReadTask[Row]] = {
      val lowerBound = filters.collect {
        case GreaterThan("i", v: Int) => v
      }.headOption

      val res = new ArrayList[ReadTask[Row]]

      if (lowerBound.isEmpty) {
        res.add(new AdvancedReadTask(0, 5, requiredSchema))
        res.add(new AdvancedReadTask(5, 10, requiredSchema))
      } else if (lowerBound.get < 4) {
        res.add(new AdvancedReadTask(lowerBound.get + 1, 5, requiredSchema))
        res.add(new AdvancedReadTask(5, 10, requiredSchema))
      } else if (lowerBound.get < 9) {
        res.add(new AdvancedReadTask(lowerBound.get + 1, 10, requiredSchema))
      }

      res
    }
  }

  override def inferSchema(options: CaseInsensitiveMap[String]): StructType = {
    new StructType().add("i", "int").add("j", "int")
  }

  override def createReader(
      schema: StructType,
      options: CaseInsensitiveMap[String]): DataSourceV2Reader = new Reader(schema)
}

class AdvancedReadTask(start: Int, end: Int, requiredSchema: StructType)
  extends ReadTask[Row] {

  private var current = start - 1
  override def next(): Boolean = {
    current += 1
    current < end
  }

  override def get(): Row = {
    val values = requiredSchema.map(_.name).map {
      case "i" => current
      case "j" => -current
    }
    Row.fromSeq(values)
  }
}



class UnsafeRowDataSourceV2 extends DataSourceV2 with DataSourceV2SchemaProvider {
  class Reader(val readSchema: StructType) extends DataSourceV2Reader {
    override def createUnsafeRowReadTasks(): JList[ReadTask[UnsafeRow]] = {
      java.util.Arrays.asList(new UnsafeRowReadTask(0, 5), new UnsafeRowReadTask(5, 10))
    }

    override def createReadTasks(): JList[ReadTask[Row]] = throw new IllegalStateException()
  }

  override def inferSchema(options: CaseInsensitiveMap[String]): StructType = {
    new StructType().add("i", "int").add("j", "int")
  }

  override def createReader(
      schema: StructType,
      options: CaseInsensitiveMap[String]): DataSourceV2Reader = new Reader(schema)
}

class UnsafeRowReadTask(start: Int, end: Int) extends ReadTask[UnsafeRow] {
  private val row = new UnsafeRow(2)
  row.pointTo(new Array[Byte](8 * 3), 8 * 3)

  private var current = start - 1
  override def next(): Boolean = {
    current += 1
    current < end
  }
  override def get(): UnsafeRow = {
    row.setInt(0, current)
    row.setInt(1, -current)
    row
  }
}



class BatchDataSourceV2 extends DataSourceV2 with DataSourceV2SchemaProvider {
  class Reader(val readSchema: StructType) extends DataSourceV2Reader with ColumnarReadSupport {
    override def createReadTasks(): JList[ReadTask[Row]] = {
      throw new NotImplementedError()
    }

    override def createColumnarReadTasks(): JList[ReadTask[ColumnarBatch]] = {
      java.util.Arrays.asList(new ColumnarReadTask(readSchema))
    }
  }

  override def inferSchema(options: CaseInsensitiveMap[String]): StructType = {
    new StructType().add("i", "int").add("j", "int")
  }

  override def createReader(
      schema: StructType,
      options: CaseInsensitiveMap[String]): DataSourceV2Reader = new Reader(schema)
}

class ColumnarReadTask(schema: StructType) extends ReadTask[ColumnarBatch] {
  private lazy val batch = ColumnarBatch.allocate(schema)
  private var currentBatch = 0

  override def next(): Boolean = {
    currentBatch += 1
    currentBatch <= 2
  }

  override def get(): ColumnarBatch = {
    batch.reset()
    if (currentBatch == 1) {
      batch.column(0).putInts(0, 5, 0.until(5).toArray, 0)
      batch.column(1).putInts(0, 5, 0.until(5).map(i => -i).toArray, 0)
    } else {
      batch.column(0).putInts(0, 5, 5.until(10).toArray, 0)
      batch.column(1).putInts(0, 5, 5.until(10).map(i => -i).toArray, 0)
    }
    batch.setNumRows(5)
    batch
  }
}
