package com.example.queue

import java.io._
import java.util.PriorityQueue
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable.ArrayBuffer
import scala.math.Ordered.orderingToOrdered
import scala.reflect.io.File


/**
 * A stream for median calculation.
 *
 * It uses a priority queue as the internal data structure to keep track of the
 * elements in the stream. The elements are stored in a sorted manner with respect
 * to the given ordering.
 *
 * The stream is divided into buckets. Each bucket is a [[DataBucket]] which is
 * a priority queue with a limited capacity. The elements in the bucket are
 * sorted by the given ordering.
 *
 * The median of the stream is calculated by maintaining a sorted order of the
 * buckets. The median is the middle element of the sorted buckets. If there are
 * two middle elements, the median is the average of the two elements.
 *
 * The median is updated lazily when the stream is iterated.
 *
 * @param bucketCapacity the capacity of each bucket
 * @param ordering       the ordering of the elements in the stream
 */
class MedianQueueStream[T](bucketCapacity: Int)(implicit ordering: Ordering[T]) {
  val buckets = new ArrayBuffer[DataBucket[T]]()

  def add(data: T): Unit = {
    if (buckets.isEmpty) {
      appendNewBucket(data)
    } else {
      // calculate the index of the data bucket
      val idx =
        if (data <= buckets.head.maxValue)
          0
        else if (data >= buckets.last.maxValue)
          buckets.size - 1
        else
          buckets.indexWhere(b => data < b.maxValue)

      var lastPop = data

      idx until buckets.size foreach { i =>
        val b = buckets(i)
        if (b.isFull) {
          val pop = b.poll()
          b.add(lastPop)
          lastPop = pop
          if (i == buckets.size - 1)
            appendNewBucket(lastPop)
        } else
          b.add(lastPop)
      }
    }
  }

  def getMedian: (T, T) = {
    if (buckets.isEmpty) throw new IllegalStateException("stream is empty")
    val total = buckets.map(_.size()).sum
    val idxBucket = total / 2 / bucketCapacity
    if (buckets.size > 1 && buckets.last.isFull && total % 2 == 0) {
      (buckets(idxBucket - 1).maxValue, buckets(idxBucket).getOrderedByOffset(0, total)._1)
    } else {
      val offset = total / 2 % bucketCapacity
      buckets(idxBucket).getOrderedByOffset(offset, total)
    }
  }

  private def appendNewBucket(data: T): Unit = {
    val bucket = DataBucket(data, bucketCapacity)
    bucket.add(data)
    buckets.append(bucket)
  }
}

/**
 * A case class representing a data bucket with a minimum value,
 * maximum value, and a specified capacity. The data bucket
 * maintains data in an ordered manner with respect to the provided
 * ordering.
 *
 * Bucket structure: minValue ++ buffer ++ maxValue
 *
 * @param maxValue the maximum value in the bucket
 * @param capacity the capacity of the bucket
 * @param ordering the implicit ordering for type T
 * @tparam T the type of elements in the bucket
 */
case class DataBucket[T](@transient var maxValue: T,
                         capacity: Int)(implicit ordering: Ordering[T]) extends PriorityQueue[T](capacity, ordering.reverse) {

  private val filename = s"bucket_${capacity}_${hashCode()}"

  /**
   * Adds a new element to the data bucket. If the bucket is full,
   * an IllegalStateException is thrown.
   *
   * If the new element is smaller than the current minimum value,
   * the current minimum is moved to the start of the buffer and
   * the new element becomes the new minimum value. Conversely, if
   * the new element is larger than the current maximum value, the
   * current maximum is moved to the end of the buffer and the new
   * element becomes the new maximum value.
   *
   * @param data the data element to add to the bucket
   * @throws IllegalStateException if the bucket is full
   */
  override def add(data: T): Boolean = {
    if (size() >= capacity) {
      throw new IllegalStateException("bucket is full")
    }
    val res = super.add(data)
    maxValue = peek()
    res
  }

  def isFull: Boolean = size() >= capacity

  /**
   * Retrieves the minimum and maximum values from the data bucket
   * sorted by the offset provided. If the offset is 0, the minimum
   * value is returned. If the offset is equal to the capacity, the
   * maximum value is returned. Otherwise, the values are retrieved
   * from the sorted buffer.
   *
   * @param offset the offset of the elements to retrieve
   * @param total  the total number of elements in the bucket
   * @return a tuple containing the minimum and maximum values sorted
   *         by the offset
   */
  def getOrderedByOffset(offset: Int, total: Long): (T, T) = {
    if (offset == capacity) (maxValue, maxValue)
    else {
      val arr = new Array[T with Object](size())
      val sorted = toArray(arr).asInstanceOf[Array[T]].sorted
      val left = math.max(offset - 1, 0)
      if (total % 2 == 0) (sorted(left), sorted(offset))
      else (sorted(offset), sorted(offset))
    }
  }

  /**
   * Writes the data bucket to a file. The file is overwritten
   * if it already exists.
   */
  @throws[IOException]
  def write(): Unit = {
    val file = File(filename)
    if (!file.exists) {
      if (!file.parent.exists) file.parent.createDirectory(force = true, failIfExists = false)
      file.createFile()
    }
    val output = new ObjectOutputStream(new FileOutputStream(filename, false))
    output.writeObject(maxValue)
    output.writeInt(size())
    this.iterator().asScala.foreach(output.writeObject)
    output.flush()
    output.close()
  }

  /**
   * Reads the data bucket from the file created by the write method.
   * It is assumed that the file is in the same state as when the write
   * method was called.
   */
  @throws[ClassCastException]
  def read(): Unit = {
    val input = new ObjectInputStream(new FileInputStream(filename))
    maxValue = input.readObject().asInstanceOf[T]
    val size = input.readInt()
    0 until size foreach (_ => add(input.readObject().asInstanceOf[T]))
    input.close()
  }
}
