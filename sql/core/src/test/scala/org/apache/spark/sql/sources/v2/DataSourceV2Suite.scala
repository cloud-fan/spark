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

import test.org.apache.spark.sql.sources.v2.{JavaAdvancedDataSourceV2, _}

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanExec}
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.{Filter, GreaterThan}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.reader.partitioning.{ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class DataSourceV2Suite extends QueryTest with SharedSQLContext {
  import testImplicits._

  private def getMetadata(query: DataFrame): AdvancedMetadata = {
    query.queryExecution.executedPlan.collect {
      case d: DataSourceV2ScanExec =>
        d.metadata.asInstanceOf[AdvancedMetadata]
    }.head
  }

  private def getJavaMetadata(query: DataFrame): JavaAdvancedDataSourceV2.AdvancedMetadata = {
    query.queryExecution.executedPlan.collect {
      case d: DataSourceV2ScanExec =>
        d.metadata.asInstanceOf[JavaAdvancedDataSourceV2.AdvancedMetadata]
    }.head
  }

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

        val q1 = df.select('j)
        checkAnswer(q1, (0 until 10).map(i => Row(-i)))
        if (cls == classOf[AdvancedDataSourceV2]) {
          val metadata = getMetadata(q1)
          assert(metadata.filters.isEmpty)
          assert(metadata.requiredSchema.fieldNames === Seq("j"))
        } else {
          val metadata = getJavaMetadata(q1)
          assert(metadata.filters.isEmpty)
          assert(metadata.requiredSchema.fieldNames === Seq("j"))
        }

        val q2 = df.filter('i > 3)
        checkAnswer(q2, (4 until 10).map(i => Row(i, -i)))
        if (cls == classOf[AdvancedDataSourceV2]) {
          val metadata = getMetadata(q2)
          assert(metadata.filters.flatMap(_.references).toSet == Set("i"))
          assert(metadata.requiredSchema.fieldNames === Seq("i", "j"))
        } else {
          val metadata = getJavaMetadata(q2)
          assert(metadata.filters.flatMap(_.references).toSet == Set("i"))
          assert(metadata.requiredSchema.fieldNames === Seq("i", "j"))
        }

        val q3 = df.select('i).filter('i > 6)
        checkAnswer(q3, (7 until 10).map(i => Row(i)))
        if (cls == classOf[AdvancedDataSourceV2]) {
          val metadata = getMetadata(q3)
          assert(metadata.filters.flatMap(_.references).toSet == Set("i"))
          assert(metadata.requiredSchema.fieldNames === Seq("i"))
        } else {
          val metadata = getJavaMetadata(q3)
          assert(metadata.filters.flatMap(_.references).toSet == Set("i"))
          assert(metadata.requiredSchema.fieldNames === Seq("i"))
        }

        val q4 = df.select('j).filter('j < -10)
        checkAnswer(q4, Nil)
        if (cls == classOf[AdvancedDataSourceV2]) {
          val metadata = getMetadata(q4)
          // 'j < 10 is not supported by the testing data source.
          assert(metadata.filters.isEmpty)
          assert(metadata.requiredSchema.fieldNames === Seq("j"))
        } else {
          val metadata = getJavaMetadata(q4)
          // 'j < 10 is not supported by the testing data source.
          assert(metadata.filters.isEmpty)
          assert(metadata.requiredSchema.fieldNames === Seq("j"))
        }
      }
    }
  }

  test("columnar batch scan implementation") {
    Seq(classOf[BatchDataSourceV2], classOf[JavaBatchDataSourceV2]).foreach { cls =>
      withClue(cls.getName) {
        val df = spark.read.format(cls.getName).load()
        checkAnswer(df, (0 until 90).map(i => Row(i, -i)))
        checkAnswer(df.select('j), (0 until 90).map(i => Row(-i)))
        checkAnswer(df.filter('i > 50), (51 until 90).map(i => Row(i, -i)))
      }
    }
  }

  test("schema required data source") {
    Seq(classOf[SchemaRequiredDataSource], classOf[JavaSchemaRequiredDataSource]).foreach { cls =>
      withClue(cls.getName) {
        val e = intercept[AnalysisException](spark.read.format(cls.getName).load())
        assert(e.message.contains("requires a user-supplied schema"))

        val schema = new StructType().add("i", "int").add("s", "string")
        val df = spark.read.format(cls.getName).schema(schema).load()

        assert(df.schema == schema)
        assert(df.collect().isEmpty)
      }
    }
  }

  test("partitioning reporting") {
    import org.apache.spark.sql.functions.{count, sum}
    Seq(classOf[PartitionAwareDataSource], classOf[JavaPartitionAwareDataSource]).foreach { cls =>
      withClue(cls.getName) {
        val df = spark.read.format(cls.getName).load()
        checkAnswer(df, Seq(Row(1, 4), Row(1, 4), Row(3, 6), Row(2, 6), Row(4, 2), Row(4, 2)))

        val groupByColA = df.groupBy('a).agg(sum('b))
        checkAnswer(groupByColA, Seq(Row(1, 8), Row(2, 6), Row(3, 6), Row(4, 4)))
        assert(groupByColA.queryExecution.executedPlan.collectFirst {
          case e: ShuffleExchangeExec => e
        }.isEmpty)

        val groupByColAB = df.groupBy('a, 'b).agg(count("*"))
        checkAnswer(groupByColAB, Seq(Row(1, 4, 2), Row(2, 6, 1), Row(3, 6, 1), Row(4, 2, 2)))
        assert(groupByColAB.queryExecution.executedPlan.collectFirst {
          case e: ShuffleExchangeExec => e
        }.isEmpty)

        val groupByColB = df.groupBy('b).agg(sum('a))
        checkAnswer(groupByColB, Seq(Row(2, 8), Row(4, 2), Row(6, 5)))
        assert(groupByColB.queryExecution.executedPlan.collectFirst {
          case e: ShuffleExchangeExec => e
        }.isDefined)

        val groupByAPlusB = df.groupBy('a + 'b).agg(count("*"))
        checkAnswer(groupByAPlusB, Seq(Row(5, 2), Row(6, 2), Row(8, 1), Row(9, 1)))
        assert(groupByAPlusB.queryExecution.executedPlan.collectFirst {
          case e: ShuffleExchangeExec => e
        }.isDefined)
      }
    }
  }

  test("SPARK-23574: no shuffle exchange with single partition") {
    val df = spark.read.format(classOf[SimpleSinglePartitionSource].getName).load().agg(count("*"))
    assert(df.queryExecution.executedPlan.collect { case e: Exchange => e }.isEmpty)
  }

  test("simple writable data source") {
    // TODO: java implementation.
    Seq(classOf[SimpleWritableDataSource]).foreach { cls =>
      withTempPath { file =>
        val path = file.getCanonicalPath
        assert(spark.read.format(cls.getName).option("path", path).load().collect().isEmpty)

        spark.range(10).select('id, -'id).write.format(cls.getName)
          .option("path", path).save()
        checkAnswer(
          spark.read.format(cls.getName).option("path", path).load(),
          spark.range(10).select('id, -'id))

        // test with different save modes
        spark.range(10).select('id, -'id).write.format(cls.getName)
          .option("path", path).mode("append").save()
        checkAnswer(
          spark.read.format(cls.getName).option("path", path).load(),
          spark.range(10).union(spark.range(10)).select('id, -'id))

        spark.range(5).select('id, -'id).write.format(cls.getName)
          .option("path", path).mode("overwrite").save()
        checkAnswer(
          spark.read.format(cls.getName).option("path", path).load(),
          spark.range(5).select('id, -'id))

        spark.range(5).select('id, -'id).write.format(cls.getName)
          .option("path", path).mode("ignore").save()
        checkAnswer(
          spark.read.format(cls.getName).option("path", path).load(),
          spark.range(5).select('id, -'id))

        val e = intercept[Exception] {
          spark.range(5).select('id, -'id).write.format(cls.getName)
            .option("path", path).mode("error").save()
        }
        assert(e.getMessage.contains("data already exists"))

        // test transaction
        val failingUdf = org.apache.spark.sql.functions.udf {
          var count = 0
          (id: Long) => {
            if (count > 5) {
              throw new RuntimeException("testing error")
            }
            count += 1
            id
          }
        }
        // this input data will fail to read middle way.
        val input = spark.range(10).select(failingUdf('id).as('i)).select('i, -'i)
        val e2 = intercept[SparkException] {
          input.write.format(cls.getName).option("path", path).mode("overwrite").save()
        }
        assert(e2.getMessage.contains("Writing job aborted"))
        // make sure we don't have partial data.
        assert(spark.read.format(cls.getName).option("path", path).load().collect().isEmpty)

        // test internal row writer
        spark.range(5).select('id, -'id).write.format(cls.getName)
          .option("path", path).option("internal", "true").mode("overwrite").save()
        checkAnswer(
          spark.read.format(cls.getName).option("path", path).load(),
          spark.range(5).select('id, -'id))
      }
    }
  }

  test("simple counter in writer with onDataWriterCommit") {
    Seq(classOf[SimpleWritableDataSource]).foreach { cls =>
      withTempPath { file =>
        val path = file.getCanonicalPath
        assert(spark.read.format(cls.getName).option("path", path).load().collect().isEmpty)

        val numPartition = 6
        spark.range(0, 10, 1, numPartition).select('id, -'id).write.format(cls.getName)
          .option("path", path).save()
        checkAnswer(
          spark.read.format(cls.getName).option("path", path).load(),
          spark.range(10).select('id, -'id))

        assert(SimpleCounter.getCounter == numPartition,
          "method onDataWriterCommit should be called as many as the number of partitions")
      }
    }
  }

  test("SPARK-23293: data source v2 self join") {
    val df = spark.read.format(classOf[SimpleDataSourceV2].getName).load()
    val df2 = df.select(($"i" + 1).as("k"), $"j")
    checkAnswer(df.join(df2, "j"), (0 until 10).map(i => Row(-i, i, i + 1)))
  }

  test("SPARK-23301: column pruning with arbitrary expressions") {
    val df = spark.read.format(classOf[AdvancedDataSourceV2].getName).load()

    val q1 = df.select('i + 1)
    checkAnswer(q1, (1 until 11).map(i => Row(i)))
    val metadata1 = getMetadata(q1)
    assert(metadata1.requiredSchema.fieldNames === Seq("i"))

    val q2 = df.select(lit(1))
    checkAnswer(q2, (0 until 10).map(i => Row(1)))
    val metadata2 = getMetadata(q2)
    assert(metadata2.requiredSchema.isEmpty)

    // 'j === 1 can't be pushed down, but we should still be able do column pruning
    val q3 = df.filter('j === -1).select('j * 2)
    checkAnswer(q3, Row(-2))
    val metadata3 = getMetadata(q3)
    assert(metadata3.filters.isEmpty)
    assert(metadata3.requiredSchema.fieldNames === Seq("j"))

    // column pruning should work with other operators.
    val q4 = df.sort('i).limit(1).select('i + 1)
    checkAnswer(q4, Row(1))
    val metadata4 = getMetadata(q4)
    assert(metadata4.requiredSchema.fieldNames === Seq("i"))
  }

  test("SPARK-23315: get output from canonicalized data source v2 related plans") {
    def checkCanonicalizedOutput(
        df: DataFrame, logicalNumOutput: Int, physicalNumOutput: Int): Unit = {
      val logical = df.queryExecution.optimizedPlan.collect {
        case d: DataSourceV2Relation => d
      }.head
      assert(logical.canonicalized.output.length == logicalNumOutput)

      val physical = df.queryExecution.executedPlan.collect {
        case d: DataSourceV2ScanExec => d
      }.head
      assert(physical.canonicalized.output.length == physicalNumOutput)
    }

    val df = spark.read.format(classOf[AdvancedDataSourceV2].getName).load()
    checkCanonicalizedOutput(df, 2, 2)
    checkCanonicalizedOutput(df.select('i), 2, 1)
  }
}


case class RangeInputSplit(start: Int, end: Int) extends InputSplit

class SimpleSinglePartitionSource extends DataSourceV2 with ReadSupport {

  class Reader extends DataSourceReader {
    override def getMetadata: Metadata = {
      new SchemaOnlyMetadata(new StructType().add("i", "int").add("j", "int"))
    }

    override def getSplitManager(meta: Metadata): SplitManager = new SplitManager {
      override def getSplits: Array[InputSplit] = {
        Array(RangeInputSplit(0, 5))
      }
    }

    override def getReaderProvider(meta: Metadata): SplitReaderProvider = {
      SimpleSplitReaderProvider
    }
  }

  override def createReader(options: DataSourceOptions): DataSourceReader = new Reader
}

class SimpleDataSourceV2 extends DataSourceV2 with ReadSupport {

  class Reader extends DataSourceReader {
    override def getMetadata: Metadata = {
      new SchemaOnlyMetadata(new StructType().add("i", "int").add("j", "int"))
    }

    override def getSplitManager(meta: Metadata): SplitManager = new SplitManager {
      override def getSplits: Array[InputSplit] = {
        Array(RangeInputSplit(0, 5), RangeInputSplit(5, 10))
      }
    }

    override def getReaderProvider(meta: Metadata): SplitReaderProvider = {
      SimpleSplitReaderProvider
    }
  }

  override def createReader(options: DataSourceOptions): DataSourceReader = new Reader
}

object SimpleSplitReaderProvider extends SplitReaderProvider {
  override def createRowReader(split: InputSplit): InputPartitionReader[InternalRow] = {
    val s = split.asInstanceOf[RangeInputSplit]

    new InputPartitionReader[InternalRow] {
      private var current = s.start - 1

      override def next(): Boolean = {
        current += 1
        current < s.end
      }

      override def get(): InternalRow = InternalRow(current, -current)

      override def close(): Unit = {}
    }
  }
}


class AdvancedDataSourceV2 extends DataSourceV2 with ReadSupport {

  class Reader extends DataSourceReader {

    override def getMetadata: Metadata = new AdvancedMetadata

    override def getSplitManager(meta: Metadata): SplitManager = new SplitManager {
      val filters = meta.asInstanceOf[AdvancedMetadata].filters

      override def getSplits: Array[InputSplit] = {
        val lowerBound = filters.collect {
          case GreaterThan("i", v: Int) => v
        }.headOption

        val res = new scala.collection.mutable.ArrayBuffer[RangeInputSplit]

        if (lowerBound.isEmpty) {
          res.append(RangeInputSplit(0, 5))
          res.append(RangeInputSplit(5, 10))
        } else if (lowerBound.get < 4) {
          res.append(RangeInputSplit(lowerBound.get + 1, 5))
          res.append(RangeInputSplit(5, 10))
        } else if (lowerBound.get < 9) {
          res.append(RangeInputSplit(lowerBound.get + 1, 10))
        }

        res.toArray
      }
    }

    override def getReaderProvider(meta: Metadata): SplitReaderProvider = {
      new AdvancedSplitReaderProvider(meta.asInstanceOf[AdvancedMetadata].requiredSchema)
    }
  }

  override def createReader(options: DataSourceOptions): DataSourceReader = new Reader
}

class AdvancedMetadata extends Metadata
  with SupportsPushDownRequiredColumns with SupportsPushDownFilters {

  var requiredSchema = new StructType().add("i", "int").add("j", "int")
  var filters = Array.empty[Filter]

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (supported, unsupported) = filters.partition {
      case GreaterThan("i", _: Int) => true
      case _ => false
    }
    this.filters = supported
    unsupported
  }

  override def pushedFilters(): Array[Filter] = filters

  override def getSchema(): StructType = {
    requiredSchema
  }
}

class AdvancedSplitReaderProvider(requiredSchema: StructType) extends SplitReaderProvider {
  override def createRowReader(split: InputSplit): InputPartitionReader[InternalRow] = {
    val s = split.asInstanceOf[RangeInputSplit]
    new InputPartitionReader[InternalRow] {
      private val start = s.start
      private val end = s.end
      private var current = start - 1

      override def close(): Unit = {}

      override def next(): Boolean = {
        current += 1
        current < end
      }

      override def get(): InternalRow = {
        val values = requiredSchema.map(_.name).map {
          case "i" => current
          case "j" => -current
        }
        InternalRow.fromSeq(values)
      }
    }
  }
}


class SchemaRequiredDataSource extends DataSourceV2 with ReadSupportWithSchema {

  class Reader(schema: StructType) extends DataSourceReader {
    override def getMetadata: Metadata = new SchemaOnlyMetadata(schema)

    override def getSplitManager(meta: Metadata): SplitManager = new SplitManager {
      override def getSplits: Array[InputSplit] = Array.empty
    }

    override def getReaderProvider(meta: Metadata): SplitReaderProvider = {
      new SplitReaderProvider {}
    }
  }

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader =
    new Reader(schema)
}


class BatchDataSourceV2 extends DataSourceV2 with ReadSupport {

  class Reader extends DataSourceReader {
    override def getMetadata: Metadata = {
      new SchemaOnlyMetadata(new StructType().add("i", "int").add("j", "int"))
    }

    override def getSplitManager(meta: Metadata): SplitManager = new SplitManager {
      override def getSplits: Array[InputSplit] = {
        Array(RangeInputSplit(0, 50), RangeInputSplit(50, 90))
      }
    }

    override def getReaderProvider(meta: Metadata): SplitReaderProvider = {
      BatchSplitReaderProvider
    }
  }

  override def createReader(options: DataSourceOptions): DataSourceReader = new Reader
}

object BatchSplitReaderProvider extends SplitReaderProvider {
  private final val BATCH_SIZE = 20

  override def supportColumnarReader(): Boolean = true

  override def createColumnarReader(split: InputSplit): InputPartitionReader[ColumnarBatch] = {
    val s = split.asInstanceOf[RangeInputSplit]
    new InputPartitionReader[ColumnarBatch] {
      private val start = s.start
      private val end = s.end
      private lazy val i = new OnHeapColumnVector(BATCH_SIZE, IntegerType)
      private lazy val j = new OnHeapColumnVector(BATCH_SIZE, IntegerType)
      private lazy val batch = new ColumnarBatch(Array(i, j))

      private var current = start

      override def next(): Boolean = {
        i.reset()
        j.reset()

        var count = 0
        while (current < end && count < BATCH_SIZE) {
          i.putInt(count, current)
          j.putInt(count, -current)
          current += 1
          count += 1
        }

        if (count == 0) {
          false
        } else {
          batch.setNumRows(count)
          true
        }
      }

      override def get(): ColumnarBatch = {
        batch
      }

      override def close(): Unit = batch.close()
    }
  }
}


class PartitionAwareDataSource extends DataSourceV2 with ReadSupport {

  class Reader extends DataSourceReader with SupportsReportPartitioning {
    override def getMetadata: Metadata = {
      new SchemaOnlyMetadata(new StructType().add("a", "int").add("b", "int"))
    }

    override def getSplitManager(meta: Metadata): SplitManager = new SplitManager {
      override def getSplits: Array[InputSplit] = {
        // Note that we don't have same value of column `a` across partitions.
        Array(
          SpecificInputSplit(Array(1, 1, 3), Array(4, 4, 6)),
          SpecificInputSplit(Array(2, 4, 4), Array(6, 2, 2)))
      }
    }

    override def getReaderProvider(meta: Metadata): SplitReaderProvider = {
      SpecificSplitReaderProvider
    }

    override def outputPartitioning(meta: Metadata): Partitioning = new MyPartitioning
  }

  class MyPartitioning extends Partitioning {
    override def numPartitions(): Int = 2

    override def satisfy(distribution: Distribution): Boolean = distribution match {
      case c: ClusteredDistribution => c.clusteredColumns.contains("a")
      case _ => false
    }
  }

  override def createReader(options: DataSourceOptions): DataSourceReader = new Reader
}

case class SpecificInputSplit(i: Array[Int], j: Array[Int]) extends InputSplit

object SpecificSplitReaderProvider extends SplitReaderProvider {
  override def createRowReader(split: InputSplit): InputPartitionReader[InternalRow] = {
    val s = split.asInstanceOf[SpecificInputSplit]
    new InputPartitionReader[InternalRow] {
      private val i = s.i
      private val j = s.j
      private var current = -1

      override def next(): Boolean = {
        current += 1
        current < i.length
      }

      override def get(): InternalRow = InternalRow(i(current), j(current))

      override def close(): Unit = {}
    }
  }
}
