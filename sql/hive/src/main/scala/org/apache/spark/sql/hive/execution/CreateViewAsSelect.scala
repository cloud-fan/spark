package org.apache.spark.sql.hive.execution

import org.apache.hadoop.hive.ql.metadata.HiveUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{AnalysisException, Row, SQLContext}
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.hive.client.HiveTable

private[hive]
case class CreateViewAsSelect(
    tableDesc: HiveTable,
    newNames: Seq[String],
    allowExisting: Boolean) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val hiveContext = sqlContext.asInstanceOf[HiveContext]
    val database = tableDesc.database
    val viewName = tableDesc.name

    if (hiveContext.catalog.tableExists(Seq(database, viewName))) {
      if (allowExisting) {
        // view already exists, will do nothing, to keep consistent with Hive
      } else {
        throw new AnalysisException(s"$database.$viewName already exists.")
      }
    } else {
      val tbl = if (newNames.nonEmpty) {
        val sb = new StringBuilder
        sb.append("SELECT ")
        for (i <- 0 until newNames.length) {
          if (i > 0) {
            sb.append(", ")
          }
          sb.append(HiveUtils.unparseIdentifier(tableDesc.schema(i).name))
          sb.append(" AS ")
          sb.append(HiveUtils.unparseIdentifier(newNames(i)))
        }
        sb.append(" FROM (")
        sb.append(tableDesc.viewText.get)
        sb.append(") ")
        sb.append(HiveUtils.unparseIdentifier(tableDesc.name))
        tableDesc.copy(viewText = Some(sb.toString))
      } else tableDesc

      hiveContext.catalog.client.createView(tbl)
    }

    Seq.empty[Row]
  }
}
