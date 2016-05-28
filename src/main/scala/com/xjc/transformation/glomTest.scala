package com.xjc.transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
def glom(): RDD[Array[T]]
Return an RDD created by coalescing all elements within each partition into an array.
 */

object glomTest {
    def main(args:Array[String]) {
        val conf = new SparkConf().setAppName("glomTest").
            setMaster("local[2]")
            val sc = new SparkContext(conf)
            val data = 1 to 10
            val rdd = sc.parallelize(data, 3)    

            val rdd1 = rdd.glom
            rdd1.collect

            sc.stop
    }
}

//res26: Array[Array[Int]] = Array(Array(1, 2, 3), Array(4, 5, 6), Array(7, 8, 9, 10))
