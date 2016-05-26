package com.xjc.transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
  def fold(zeroValue: T)(op: (T, T) ⇒ T): T
  Aggregate the elements of each partition, and then the results for all the partitions, using a given associative and commutative function and a neutral "zero value".
 */

object foldTest {
    def main(args:Array[String]) {
        val conf = new SparkConf().setAppName("foldTest").
            setMaster("local[2]")
            val sc = new SparkContext(conf)
            val data = 1 to 10
            val rdd = sc.parallelize(data, 2)    

            rdd.fold(0)(_ + _)           //55
            rdd.fold(3)(_ + _)           //64

            val rdd1 = rdd.coalesce(5, true)
            rdd1.fold(3)(_ + _)           //73

            val rdd2 = rdd.coalesce(8, true)
            rdd2.fold(3)(_ + _)           //82

            sc.stop
    }
}
