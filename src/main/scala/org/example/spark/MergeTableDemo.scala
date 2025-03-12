package org.example.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.example.spark.sql.parser.MergeTableSqlParser

class MergeTableDemo {

  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("merge table").set("spark.sql.sources.partitionColumnTypeInference.enabled", "true")

  lazy val ss = SparkSession.builder()
    .withExtensions(extension => extension.injectParser((sparkSession, _) => new MergeTableSqlParser(sparkSession)))
    .config(sparkConf)
    .getOrCreate()

  def createTable() = {
    ss.read.options(Map(CSVOptions.HEADER -> "true")).csv("20250214.csv")
      .write.mode(SaveMode.Overwrite).partitionBy("trade_date").saveAsTable("trade")
    ss.sql("select count(*) from trade").show()
  }

  def readTable() = {
    ss.read.parquet("spark-warehouse/trade")
  }

  def mergeTableWithPartition() = {
    ss.sql("alter table trade merge partition(trade_date=20250214)").show()
  }

  def mergeTable(): Unit = {
    ss.sql("alter table trade merge;");
  }

}
