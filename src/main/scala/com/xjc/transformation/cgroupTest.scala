package com.xjc.transformation

import com.xjc.utils.getPartitionsDataTest._
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * def cogroup(otherDataset, [numTasks])
 * When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (Iterable<V>, Iterable<W>)) tuples. This operation is also called groupWith.
 */

object cgroupTest {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("cgroupTest").
    setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val rdd1 = sc.parallelize(Seq("a"->1, "b"->2, "c"->3, "d"->4, "e"->5), 4)
    val rdd2 = sc.parallelize(Seq("b"->22, "c"->33, "d"->44), 2)

    val rdd3 = rdd1.cogroup(rdd2)
    rdd3.foreach(println)

    sc.stop
  }
}

/**
 (d,(CompactBuffer(4),CompactBuffer(44)))
 (e,(CompactBuffer(5),CompactBuffer()))
 (a,(CompactBuffer(1),CompactBuffer()))
 (b,(CompactBuffer(2),CompactBuffer(22)))
 (c,(CompactBuffer(3),CompactBuffer(33)))
 */
