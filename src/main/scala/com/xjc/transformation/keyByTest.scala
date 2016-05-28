package com.xjc.transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
  def keyBy[K](f: (T) ⇒ K): RDD[(K, T)]
  Creates tuples of the elements in this RDD by applying f.
 */

object keyByTest {
    def main(args:Array[String]) {
        val conf = new SparkConf().setAppName("keyByTest").
            setMaster("local[2]")
            val sc = new SparkContext(conf)
            val a = sc.parallelize(List("a"->1,"b"->2,"c"->3,"d"->4, "a"->2))

            a.keyBy(_._1).collect
            /**
            res45: Array[(String, (String, Int))] = Array((a,(a,1)), (b,(b,2)), (c,(c,3)), (d,(d,4)), (a,(a,2)))
            */
            a.keyBy(_._2 + 1).collect
            /**
            res46: Array[(Int, (String, Int))] = Array((2,(a,1)), (3,(b,2)), (4,(c,3)), (5,(d,4)), (3,(a,2)))
            */

            val b = sc.parallelize(1 to 10)
            b.keyBy(_ * 10).collect
            /**
            res50: Array[(Int, Int)] = Array((10,1), (20,2), (30,3), (40,4), (50,5), (60,6), (70,7), (80,8), (90,9), (100,10))
            */

            b.keyBy(i=>i + "_1").collect
            /**
            res53: Array[(String, Int)] = Array((1_1,1), (2_1,2), (3_1,3), (4_1,4), (5_1,5), (6_1,6), (7_1,7), (8_1,8), (9_1,9), (10_1,10))
            */
            

            sc.stop
    }
}
