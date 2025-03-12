package org.example.spark.sql.parser

import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}


/**
 * 合并表分区中的小文件为1个
 *
 * @param db
 * @param table
 * @param partitions
 */
case class MergeTableCommand(db: Option[String], table: String, partitions: Map[String, String]) extends LeafRunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val partitionSpec = if (partitions.isEmpty) {
      Seq(sparkSession.table(table).coalesce(1))
    } else {
      partitions.map { case (k, v) =>
        sparkSession.table(table).where(s"${k}=${v}").coalesce(1)
      }
    }
    if (partitionSpec.nonEmpty) {
      if (partitionSpec.size > 1) {
        partitionSpec.reduceLeft((a, b) => a.union(b)).write.mode(SaveMode.Overwrite).saveAsTable(table)
      } else {
        partitionSpec.head.write.mode(SaveMode.Overwrite).saveAsTable(table)
      }
    }
    Seq.empty
  }
}
