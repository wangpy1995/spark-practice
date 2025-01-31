package com.example.queue

import org.scalatest.funsuite.AnyFunSuite

class MedianQueueStreamTest extends AnyFunSuite {
  lazy val medianQueueStream = new MedianQueueStream[Int](3)
  test("add") {
    //    val seq = 0 until 13 map (_ => Random.nextInt(100))
    val seq = Seq(9, 13, 1, 6, 51, 41, 62, 59, 78, 42, 61, 46, 25)
    println(seq.mkString(","))
    seq.foreach(s => medianQueueStream.add(s))
    medianQueueStream.buckets.map(_.toArray.mkString(",")).foreach(println)
  }

  test("get Median") {
    val seq = Seq(9, 13, 1, 6, 51, 41, 62, 59, 78, 42, 61, 46, 25)
    println(seq.mkString(","))
    seq.foreach { s =>
      medianQueueStream.add(s)
      println(medianQueueStream.getMedian)
    }
    println(seq.sorted.mkString(","))
  }
}

//Generated with love by TestMe :) Please raise issues & feature requests at: https://weirddev.com/forum#!/testme