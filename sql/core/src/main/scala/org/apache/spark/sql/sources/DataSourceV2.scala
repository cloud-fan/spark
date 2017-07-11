package org.apache.spark.sql.sources

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, JoinedRow}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Statistics}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.execution.vectorized.ColumnarBatch
import org.apache.spark.sql.types.StructType

abstract class DataSourceV2 {
  def inferSchema(
      session: SparkSession,
      userSpecifiedSchema: Option[StructType],
      options: Map[String, String]): Option[StructType] = userSpecifiedSchema

  def getReader(
      session: SparkSession,
      schema: StructType,
      options: Map[String, String]): DataSourceReader

  protected def writeRows(
      session: SparkSession,
      data: RDD[Row],
      mode: SaveMode, // shall we put `mode` into `options` as string?
      options: Map[String, String]): Unit

  def writeBatches(
      session: SparkSession,
      data: RDD[ColumnarBatch],
      mode: SaveMode,
      options: Map[String, String]): Unit = ??? // implemented with `writeRows`
}

abstract class DataSourceReader {
  def withRequiredColumns(requiredColumns: Array[String]): Option[DataSourceReader] = None

  def withFilter(filters: Array[Filter]): DataSourceReader = this

  def unhandledFilters(filters: Array[Filter]): Array[Filter] = filters

  def withHashPartition(partitionColumns: Array[String]): Option[DataSourceReader] = None

  def sizeInBytes(): Option[Long] = None

  // This is the minimal requirement for a data source implementation: a simple full scan.
  protected def readRows(): RDD[Row]

  def readBatches(): RDD[ColumnarBatch] = ??? // implemented with `readRows`
}


case class DataSourceRelation(output: Seq[Attribute], reader: DataSourceReader) extends LeafNode {
  override def computeStats(): Statistics = {
    val sizeInBytes = reader.sizeInBytes().getOrElse(conf.defaultSizeInBytes)
    Statistics(sizeInBytes = sizeInBytes)
  }
}


abstract class FileFormatDataSource extends DataSourceV2 {
  protected def inferDataSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType]

  override def inferSchema(
      session: SparkSession,
      userSpecifiedSchema: Option[StructType],
      options: Map[String, String]): Option[StructType] = {
    val partitionColumns = null // read from stuct field metadata
    // mostly the code in `DataSource.getOrInferFileFormatSchema`
  }


  def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow]

  final override def getReader(
      session: SparkSession,
      schema: StructType,
      options: Map[String, String]): DataSourceReader = {
    val partitionColumns = null // read from stuct field metadata
    val bucketSpec = null // read from stuct field metadata.

    val tableName: Option[TableIdentifier] = null // read from options
    val catalogTable = tableName.map(session.sessionState.catalog.getTableMetadata)
    val fileIndex = if (session.sqlContext.conf.manageFilesourcePartitions &&
      catalogTable.isDefined && catalogTable.get.tracksPartitionsInCatalog) {
      new CatalogFileIndex()
    } else {
      new InMemoryFileIndex() // TODO: how to share FileStatusCache between infer schema and here?
    }


    new FileFormatDataSourceReader()
  }


  protected def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory

  override def writeRows(
      session: SparkSession,
      data: RDD[Row],
      mode: SaveMode,
      options: Map[String, String]): Unit = {
    val partitionColumns = null // read from stuct field metadata
    val bucketSpec = null // read from stuct field metadata.

    val tableName: Option[TableIdentifier] = null // read from options
    val catalogTable = tableName.map(session.sessionState.catalog.getTableMetadata)

    // mostly the code in `InsertIntoHadoopFsRelationCommand`
  }
}

case class FileFormatDataSourceReader(
    session: SparkSession,
    dataSource: FileFormatDataSource,
    fileIndex: FileIndex,
    dataSchema: StructType,
    partitionSchema: StructType,
    bucketSpec: Option[BucketSpec],
    options: Map[String, String])(
    requiredColumns: Array[String] = dataSchema.map(_.name).toArray,
    filters: Seq[Filter] = Nil,
    partitionColumns: Seq[String] = Nil) extends DataSourceReader {

  override def withRequiredColumns(requiredColumns: Array[String]): Option[DataSourceReader] = {
    Some(copy()(
      requiredColumns = requiredColumns,
      filters = this.filters,
      partitionColumns = this.partitionColumns))
  }

  override def withFilter(filters: Array[Filter]): DataSourceReader = {
    // mostly the code in `PruneFileSourcePartitions`, to get new fileIndex
    val newFileIndex = null
    val dataFilters = null // remove partition filters from `filters`
    copy(fileIndex = newFileIndex)(
      requiredColumns = this.requiredColumns,
      filters = dataFilters,
      partitionColumns = this.partitionColumns)
  }

  override def withHashPartition(partitionColumns: Array[String]): Option[DataSourceReader] = {
    Some(copy()(
      requiredColumns = this.requiredColumns,
      filters = this.filters,
      partitionColumns = partitionColumns))
  }

  override def sizeInBytes(): Option[Long] = {
    Some(fileIndex.sizeInBytes)
  }

  override protected def readRows(): RDD[Row] = {
    // mostly the code in `FileSourceScanExec`
  }
}