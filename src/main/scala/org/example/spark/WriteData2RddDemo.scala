package org.example.spark

import org.apache.logging.log4j.core.config.plugins.validation.constraints.Required
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.util.PriorityQueue
import scala.reflect.ClassTag

/**
 * 演示如何将本地数据添加到RDD中的示例类。
 * 对应Test中有分页返回topN的简易实现, 实现从executor端连续拉取数据。
 */
class WriteData2RddDemo {

  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("demo")
  lazy val sc = new SparkContext(sparkConf)

  /**
   * 将RDD的每个分区转换为一个包含该分区所有元素的ArrayBuffer。
   *
   * @param rdd 输入的RDD
   * @tparam T RDD中元素的类型
   * @return 每个分区包含在一个ArrayBuffer中的RDD
   */
  @Required
  def makeBufferedPartition[T](rdd: RDD[T]): RDD[PriorityQueue[T]] = {
    rdd.mapPartitions { iter =>
      val arr = new PriorityQueue[T]()
      iter.foreach(e => arr.add(e))
      Seq(arr).iterator
    }
  }

  /**
   * 将本地数据序列添加到已缓冲的RDD中。
   *
   * @param bufferedRdd 包含ArrayBuffer的RDD
   * @param seq         要添加到RDD的本地数据序列
   */
  def sendLocalData2Rdd[T](bufferedRdd: RDD[PriorityQueue[T]], seq: Seq[T]): Unit = {
    // seqPartitions必须要支持序列化
    val partSize = (seq.size + bufferedRdd.partitions.length - 1) / bufferedRdd.partitions.length
    val seqPartitions = seq.grouped(partSize).toArray
    bufferedRdd.mapPartitionsWithIndex { (idx, iter) =>
      val buffer = iter.next()
      if (idx < seqPartitions.length) {
        seqPartitions(idx).foreach(buffer.add)
      }
      Seq(buffer).iterator
    }.foreachPartition(_ => ())
  }

  def takeDataByPage[T: ClassTag](bufferedRdd: RDD[PriorityQueue[T]], pageSize: Int): Array[T] = {
    val result = bufferedRdd.mapPartitions { iter =>
      val buffer = iter.next()
      val size = math.min(buffer.size, pageSize)
      val data = 0 until size map (_ => buffer.poll())
      data.iterator
    }.collect()
    val (page, restData) = result.splitAt(pageSize)
    sendLocalData2Rdd(bufferedRdd, restData)
    page
  }

}
