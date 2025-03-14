package org.example.format

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.funsuite.AnyFunSuite

import java.util.Scanner

class FixedLineFileFormatTestTestSuite extends AnyFunSuite {
  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("demo")
  lazy val sc = new SparkContext(sparkConf)
  test("combined Splits") {
    val conf = new Configuration()
    conf.set(FileInputFormat.INPUT_DIR,
      "file:///G:/IdeaProjects/spark-practice/spark-warehouse/utf8/2.csv,file:///G:/IdeaProjects/spark-practice/spark-warehouse/utf8/20250214.csv,file:///G:/IdeaProjects/spark-practice/spark-warehouse/utf8/《天神禁条》（校对版全本）作者：无来_UTF8.txt")
    //    conf.set(FileInputFormat.INPUT_DIR_RECURSIVE, "true")
    //    conf.set(FixedLineFileFormat.ENCODING, "GBK")
    conf.set(FileInputFormat.SPLIT_MAXSIZE, "1000000")
    val count = sc.newAPIHadoopRDD(conf, classOf[CombinedFixedLineFileFormat], classOf[LongWritable], classOf[Text]).map { case (_, value) =>
      value.toString
    }.count()
    println(count)
    new Scanner(System.in).nextLine()
  }

  test("splits") {
    val conf = new Configuration()
    conf.set(FileInputFormat.INPUT_DIR,
      "file:///G:/IdeaProjects/spark-practice/spark-warehouse/utf8/2.csv,file:///G:/IdeaProjects/spark-practice/spark-warehouse/utf8/20250214.csv,file:///G:/IdeaProjects/spark-practice/spark-warehouse/utf8/《天神禁条》（校对版全本）作者：无来_UTF8.txt")
    //    conf.set(FileInputFormat.INPUT_DIR_RECURSIVE, "true")
    //    conf.set(FixedLineFileFormat.ENCODING, "GBK")
    conf.set(FileInputFormat.SPLIT_MAXSIZE, "1000000")
    val count = sc.newAPIHadoopRDD(conf, classOf[FixedLineFileFormat], classOf[LongWritable], classOf[Text]).map { case (_, value) =>
      value.toString
    }.count()
    println(count)
    new Scanner(System.in).nextLine()
  }
}

//Generated with love by TestMe :) Please raise issues & feature requests at: https://weirddev.com/forum#!/testme