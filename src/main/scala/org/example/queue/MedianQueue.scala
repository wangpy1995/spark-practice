package org.example.queue

class MedianQueue extends Serializable {

}

case class LimitedSizeMinMaxSeq[T](var minValue: T
                                   , var maxValue: T
                                   , capacity: Int)