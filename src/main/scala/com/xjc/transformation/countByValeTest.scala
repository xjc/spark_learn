package com.xjc.transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
def countByValue()(implicit ord: Ordering[T] = null): Map[T, Long]
Return the count of each unique value in this RDD as a local map of (value, count) pairs.
*/

object countByValeTest {
    def main(args:Array[String]) {
        val conf = new SparkConf().setAppName("countByValeTest").setMaster("local[2]")
            val sc = new SparkContext(conf)
            val rdd = sc.parallelize(List(1 to 10, 5 to 10)).flatMap(i=>i)
            rdd.partitions
            rdd.countByValue().foreach(println)
            /**
              (5,2)
              (10,2)
              (1,1)
              (6,2)
              (9,2)
              (2,1)
              (7,2)
              (3,1)
              (8,2)
              (4,1)
             */

            val rdd1 = sc.textFile("file:///home/xjc/abc.txt").flatMap(_.split("\\s"))
            rdd1.countByValue().foreach(println)

            sc.stop
    }
}
