package org.example.spark

import org.scalatest.funsuite.AnyFunSuite

class WriteData2RddDemoTest extends AnyFunSuite {

  lazy val writeData2RddDemo = new WriteData2RddDemo()

  test("make Buffered Partition") {

  }


  test("write Data2 Rdd") {

  }

  test("add Local Data2 Rdd") {
    val rdd = writeData2RddDemo.sc.makeRDD(0 until 1000)
    val localData = 1000 until 2000 map (a => a)
    val bufferedRdd = writeData2RddDemo.makeBufferedPartition(rdd)
    bufferedRdd.cache()
    println("buffered rdd count: " + bufferedRdd.map(_.size).sum())
    writeData2RddDemo.sendLocalData2Rdd(bufferedRdd, localData)
    println("addLocalData2Rdd, buffered rdd count: " + bufferedRdd.map(_.size).sum())
  }

  test("take Data By Page") {
    val rdd = writeData2RddDemo.sc.makeRDD(0 until 1000)
    val bufferedRdd = writeData2RddDemo.makeBufferedPartition(rdd)
    bufferedRdd.cache()
    // 每次取10条
    0 until 10 foreach { pageNum =>
      val page = writeData2RddDemo.takeDataByPage(bufferedRdd, pageSize = 10)
      println(s"page $pageNum:\n  ${page.mkString(",")}")
    }
  }


}

//Generated with love by TestMe :) Please raise issues & feature requests at: https://weirddev.com/forum#!/testme