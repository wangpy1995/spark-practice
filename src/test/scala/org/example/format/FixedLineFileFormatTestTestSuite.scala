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
  test("get Splits") {
    val conf = new Configuration()
    conf.set(FileInputFormat.INPUT_DIR, "file:///G:/IdeaProjects/spark-practice/spark-warehouse/《天神禁条》（校对版全本）作者：无来_UTF8.txt")
//    conf.set(FixedLineFileFormat.ENCODING, "GBK")
    conf.set(FileInputFormat.SPLIT_MAXSIZE, "100000")
    val count = sc.newAPIHadoopRDD(conf, classOf[FixedLineFileFormat], classOf[LongWritable], classOf[Text]).map { case (_, value) =>
      value.toString
    }.count()
    println(count)
    new Scanner(System.in).nextLine()
  }
}

//Generated with love by TestMe :) Please raise issues & feature requests at: https://weirddev.com/forum#!/testme