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

package org.apache.spark.sql.execution.datasources.v2

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalog.v2.{Identifier, TableCatalog, TableChange}
import org.apache.spark.sql.catalog.v2.expressions.{BucketTransform, FieldReference, IdentityTransform, LogicalExpressions, Transform}
import org.apache.spark.sql.catalog.v2.utils.CatalogV2Util
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogTableType, CatalogUtils, SessionCatalog}
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.internal.{SessionState, SQLConf}
import org.apache.spark.sql.sources.v2.{Table, TableProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A [[TableCatalog]] that translates calls to the v1 SessionCatalog.
 */
class V2SessionCatalog(sessionState: SessionState) extends TableCatalog {
  def this() = {
    this(SparkSession.active.sessionState)
  }

  private lazy val catalog: SessionCatalog = sessionState.catalog

  private var _name: String = _

  override def name: String = _name

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this._name = name
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    namespace match {
      case Array(db) =>
        catalog.listTables(db).map(ident => Identifier.of(Array(db), ident.table)).toArray
      case _ =>
        throw new NoSuchNamespaceException(namespace)
    }
  }

  override def tableExists(ident: Identifier): Boolean = {
    if (ident.namespace().length <= 1) {
      catalog.tableExists(ident.asTableIdentifier)
    } else {
      false
    }
  }

  override def loadTable(ident: Identifier): Table = {
    val catalogTable = try {
      catalog.getTableMetadata(ident.asTableIdentifier)
    } catch {
      case _: NoSuchTableException =>
        throw new NoSuchTableException(ident)
    }

    V2SessionCatalog.convertV1TableToV2(catalogTable, sessionState.conf)
  }

  override def invalidateTable(ident: Identifier): Unit = {
    catalog.refreshTable(ident.asTableIdentifier)
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {

    val provider = properties.getOrDefault("provider", sessionState.conf.defaultDataSourceName)

    val (actualSchema, actualPartitions) = if (schema.isEmpty && partitions.isEmpty) {
      // If `CREATE TABLE ... USING` does not specify table metadata, get the table metadata from
      // data source first.
      val cls = DataSource.lookupDataSource(provider, sessionState.conf)
      // A sanity check. This is guaranteed by `DataSourceResolution`.
      assert(classOf[TableProvider].isAssignableFrom(cls))

      val table = cls.newInstance().asInstanceOf[TableProvider].getTable(
        new CaseInsensitiveStringMap(properties))
      table.schema() -> table.partitioning()
    } else {
      schema -> partitions
    }

    val (partitionColumns, maybeBucketSpec) = V2SessionCatalog.convertTransforms(actualPartitions)
    val tableProperties = properties.asScala
    val location = Option(properties.get("location"))
    val storage = DataSource.buildStorageFormatFromOptions(tableProperties.toMap)
        .copy(locationUri = location.map(CatalogUtils.stringToURI))

    val tableDesc = CatalogTable(
      identifier = ident.asTableIdentifier,
      tableType = CatalogTableType.MANAGED,
      storage = storage,
      schema = actualSchema,
      provider = Some(provider),
      partitionColumnNames = partitionColumns,
      bucketSpec = maybeBucketSpec,
      properties = tableProperties.toMap,
      tracksPartitionsInCatalog = sessionState.conf.manageFilesourcePartitions,
      comment = Option(properties.get("comment")))

    try {
      catalog.createTable(tableDesc, ignoreIfExists = false)
    } catch {
      case _: TableAlreadyExistsException =>
        throw new TableAlreadyExistsException(ident)
    }

    loadTable(ident)
  }

  override def alterTable(
      ident: Identifier,
      changes: TableChange*): Table = {
    val catalogTable = try {
      catalog.getTableMetadata(ident.asTableIdentifier)
    } catch {
      case _: NoSuchTableException =>
        throw new NoSuchTableException(ident)
    }

    val properties = CatalogV2Util.applyPropertiesChanges(catalogTable.properties, changes)
    val schema = CatalogV2Util.applySchemaChanges(catalogTable.schema, changes)

    try {
      catalog.alterTable(catalogTable.copy(properties = properties, schema = schema))
    } catch {
      case _: NoSuchTableException =>
        throw new NoSuchTableException(ident)
    }

    loadTable(ident)
  }

  override def dropTable(ident: Identifier): Boolean = {
    if (tableExists(ident)) {
      catalog.dropTable(
        ident.asTableIdentifier,
        ignoreIfNotExists = true,
        purge = true /* skip HDFS trash */)
      true
    } else {
      false
    }
  }

  implicit class TableIdentifierHelper(ident: Identifier) {
    def asTableIdentifier: TableIdentifier = {
      ident.namespace match {
        case Array(db) =>
          TableIdentifier(ident.name, Some(db))
        case Array() =>
          TableIdentifier(ident.name, Some(catalog.getCurrentDatabase))
        case _ =>
          throw new NoSuchTableException(ident)
      }
    }
  }

  override def toString: String = s"V2SessionCatalog($name)"
}

private[sql] object V2SessionCatalog {
  /**
   * Convert v2 Transforms to v1 partition columns and an optional bucket spec.
   */
  private def convertTransforms(partitions: Seq[Transform]): (Seq[String], Option[BucketSpec]) = {
    val identityCols = new mutable.ArrayBuffer[String]
    var bucketSpec = Option.empty[BucketSpec]

    partitions.map {
      case IdentityTransform(FieldReference(Seq(col))) =>
        identityCols += col

      case BucketTransform(numBuckets, FieldReference(Seq(col))) =>
        bucketSpec = Some(BucketSpec(numBuckets, col :: Nil, Nil))

      case transform =>
        throw new UnsupportedOperationException(
          s"SessionCatalog does not support partition transform: $transform")
    }

    (identityCols, bucketSpec)
  }

  /**
   * Convert a v1 [[CatalogTable]] to a v2 [[Table]], if the v1 table's provider is a Data Source
   * V2 implementation.
   */
  def convertV1TableToV2(v1Table: CatalogTable, conf: SQLConf): Table = {
    assert(v1Table.provider.isDefined)
    val cls = DataSource.lookupDataSource(v1Table.provider.get, conf)
    if (!classOf[TableProvider].isAssignableFrom(cls)) {
      throw new AnalysisException(s"${cls.getName} is not a valid Spark SQL Data Source.")
    }

    val provider = cls.newInstance().asInstanceOf[TableProvider]

    val options: Map[String, String] = {
      v1Table.storage.locationUri match {
        case Some(uri) =>
          v1Table.storage.properties + ("path" -> uri.toString)
        case _ =>
          v1Table.storage.properties
      }
    }

    val partitioning: Array[Transform] = {
      val partitions = new mutable.ArrayBuffer[Transform]()

      v1Table.partitionColumnNames.foreach { col =>
        partitions += LogicalExpressions.identity(col)
      }

      v1Table.bucketSpec.foreach { spec =>
        partitions += LogicalExpressions.bucket(spec.numBuckets, spec.bucketColumnNames: _*)
      }

      partitions.toArray
    }

    val v2Options = new CaseInsensitiveStringMap(options.asJava)
    try {
      provider.getTable(v2Options, v1Table.schema, partitioning)
    } catch {
      case _: UnsupportedOperationException =>
        // If the table can't accept user-specified schema and partitions, get the table via
        // options directly.
        val table = provider.getTable(v2Options)
        if (table.schema() != v1Table.schema ||
          !table.partitioning().sameElements(partitioning)) {
          throw new IllegalArgumentException("The table metadata has been updated externally, " +
            s"please re-create table ${v1Table.qualifiedName}")
        }
        table
    }
  }
}
