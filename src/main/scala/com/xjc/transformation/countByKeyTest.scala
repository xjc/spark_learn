package com.xjc.transformation

import com.xjc.utils.getPartitionsDataTest._
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * def countByKey(): Map[K, Long]
 * Count the number of elements for each key, collecting the results to a local Map.
 *
 * Note that this method should only be used if the resulting map is expected to be small, as the whole thing is loaded into the driver's memory. To handle very large results, consider using rdd.mapValues(_ => 1L).reduceByKey(_ + _), which returns an RDD[T, Long] instead of a map.
   */

  object countByKeyTest {
    def main(args:Array[String]) {
      val conf = new SparkConf().setAppName("countByKeyTest").
      setMaster("local[2]")
      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")

      val rdd1 = sc.parallelize(Seq("a"->1, "b"->2, "c"->3, "d"->4, "e"->5, "a"->2, "a"->3), 4)
      val rdd1_1 = rdd1.countByKey()
      rdd1_1.foreach(println)

      sc.stop
    }
  }

  /**
   * (e,1)
   * (a,3)
   * (b,1)
   * (c,1)
   * (d,1)
   */
